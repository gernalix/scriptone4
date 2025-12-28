# v17
import os
import sys
import time
import sqlite3
from datetime import datetime
from typing import Any, Dict, List
import json
import re
import io
import zipfile
import hashlib
import random
import shutil

from memento_sdk import (
    fetch_incremental_pages,
    fetch_all_entries_full,
)

# ---------------------------------------------------------------------
# Logging helpers (timestamp everywhere)
# ---------------------------------------------------------------------

def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def log(msg: str):
    print(f"[{ts()}] {msg}")

def log_error(msg: str):
    print(f"[{ts()}] [ERRORE] {msg}")

# ---------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------

def flatten_entries(entries: Any) -> List[Dict]:
    """
    Normalizza payload API:
    - dict -> [dict]
    - list di dict -> ok
    - list annidate -> flatten
    """
    out = []

    def _walk(x):
        if isinstance(x, dict):
            out.append(x)
        elif isinstance(x, list):
            for y in x:
                _walk(y)
        else:
            # forma sconosciuta, ignora
            pass

    _walk(entries)
    return out

# ---------------------------------------------------------------------
# Sync state (checkpoint)
# ---------------------------------------------------------------------

def ensure_sync_state_table(conn) -> None:
    """Crea la tabella di checkpoint per gli import incremental (se manca).
    Mantiene uno stato per library_id -> last_modified_remote."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memento_sync (
            library_id TEXT PRIMARY KEY,
            last_modified_remote TEXT
        )
        """
    )
    conn.commit()

def load_sync_state(conn, library_id: str):
    cur = conn.cursor()
    cur.execute(
        "SELECT last_modified_remote FROM memento_sync WHERE library_id=?",
        (library_id,),
    )
    row = cur.fetchone()
    return row[0] if row else None

def save_sync_state(conn, library_id: str, last_modified_remote: str):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO memento_sync (library_id, last_modified_remote)
        VALUES (?, ?)
        ON CONFLICT(library_id)
        DO UPDATE SET last_modified_remote=excluded.last_modified_remote
        """,
        (library_id, last_modified_remote),
    )
    conn.commit()
    log(f"✓ Checkpoint scritto: {last_modified_remote}")

# ---------------------------------------------------------------------
# Import core
# ---------------------------------------------------------------------


def _normalize_key(s: str) -> str:
    import re
    return re.sub(r"[\s\._-]+", "", str(s).strip().lower())


def _find_csv_zip(search_dir: str) -> str:
    p = os.path.join(search_dir, "memento_all_csv.zip")
    return p if os.path.exists(p) else ""


def _load_autodetect_cache(cache_path: str) -> Dict[str, Any]:
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_autodetect_cache(cache_path: str, data: Dict[str, Any]) -> None:
    try:
        tmp = cache_path + ".tmp"
        with open(tmp, "w", encoding="utf-8", newline="\n") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, cache_path)
    except Exception:
        pass


def _persist_ini_enrich_details(ini_path: str, section: str, value: bool) -> None:
    """
    Aggiorna/aggiunge la riga 'enrich_details=...' SOLO nella sezione target,
    preservando il resto del file il più possibile (no configparser write()).
    Crea anche un backup .bak una sola volta (se non esiste).
    """
    try:
        if not os.path.exists(ini_path):
            return
        bak = ini_path + ".bak"
        if not os.path.exists(bak):
            try:
                shutil.copy2(ini_path, bak)
            except Exception:
                pass

        with open(ini_path, "r", encoding="utf-8") as f:
            text = f.read()

        sec_pat = re.compile(rf"(?ms)^\[{re.escape(section)}\]\s*(.*?)(?=^\[|\Z)")
        m = sec_pat.search(text)
        if not m:
            return

        block = m.group(0)
        body = m.group(1)

        new_line = f"enrich_details={'true' if value else 'false'}"

        # Replace if present, otherwise insert after section header
        if re.search(r"(?mi)^\s*enrich_details\s*=", body):
            body2 = re.sub(r"(?mi)^\s*enrich_details\s*=\s*.*$", new_line, body)
        else:
            # insert at end of block body (before next section)
            body2 = body.rstrip() + "\n" + new_line + "\n"

        new_block = f"[{section}]\n" + body2.lstrip("\n")
        text2 = text[:m.start()] + new_block + text[m.end():]

        tmp = ini_path + ".tmp"
        with open(tmp, "w", encoding="utf-8", newline="\n") as f:
            f.write(text2)
        os.replace(tmp, ini_path)
    except Exception:
        pass


def _open_csv_from_zip(zip_path: str, member: str) -> List[List[str]]:
    import csv as _csv
    with zipfile.ZipFile(zip_path, "r") as z:
        raw = z.read(member)
    # Decode robustly
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            s = raw.decode(enc)
            break
        except Exception:
            s = raw.decode("utf-8", errors="replace")
    # Sniff delimiter
    sample = s[:4096]
    try:
        dialect = _csv.Sniffer().sniff(sample, delimiters=",;\t")
        delim = dialect.delimiter
    except Exception:
        delim = ","
    reader = _csv.reader(io.StringIO(s), delimiter=delim)
    return [row for row in reader if row]


def _load_csv_header_and_sample(zip_path: str, section: str, table: str, library_id: str) -> Dict[str, Any]:
    """
    Ritorna:
      - member (nome file dentro zip)
      - headers (lista)
      - pk (colonna id se trovata)
      - sample_by_id (dict id -> rowdict) se pk trovato
      - signature (hash header)
    """
    with zipfile.ZipFile(zip_path, "r") as z:
        members = [n for n in z.namelist() if n.lower().endswith(".csv")]

    # Build normalized index
    idx = {}
    for n in members:
        stem = os.path.basename(n)[:-4]
        idx[_normalize_key(stem)] = n

    key_candidates = [
        _normalize_key(table),
        _normalize_key(section),
        _normalize_key(library_id),
    ]
    member = ""
    for k in key_candidates:
        if k and k in idx:
            member = idx[k]
            break
    if not member:
        # fuzzy contains
        for n in members:
            stem = os.path.basename(n)[:-4]
            ns = _normalize_key(stem)
            if _normalize_key(table) and _normalize_key(table) in ns:
                member = n
                break
            if _normalize_key(section) and _normalize_key(section) in ns:
                member = n
                break
            if _normalize_key(library_id) and _normalize_key(library_id) in ns:
                member = n
                break

    if not member:
        return {"member": "", "headers": [], "pk": "", "sample_by_id": {}, "signature": ""}

    rows = _open_csv_from_zip(zip_path, member)
    if not rows:
        return {"member": member, "headers": [], "pk": "", "sample_by_id": {}, "signature": ""}

    headers = rows[0]
    sig = hashlib.sha1(("|".join(headers)).encode("utf-8", errors="ignore")).hexdigest()[:12]

    # infer pk
    pk = ""
    for h in headers:
        hl = _normalize_key(h)
        if hl in ("id", "entryid", "entry_id", "mementoid", "memento_id"):
            pk = h
            break

    sample_by_id = {}
    if pk:
        # sample first 30 + up to 30 random
        data_rows = rows[1:]
        head = data_rows[:30]
        tail = random.sample(data_rows, k=min(30, len(data_rows))) if len(data_rows) > 30 else []
        sample = head + tail
        pk_idx = headers.index(pk)
        for r in sample:
            if pk_idx < len(r):
                rid = str(r[pk_idx]).strip()
                if rid:
                    sample_by_id[rid] = {headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))}

    return {"member": member, "headers": headers, "pk": pk, "sample_by_id": sample_by_id, "signature": sig}


def _ensure_table_schema(conn, table: str, headers: List[str], pk: str) -> None:
    cur = conn.cursor()
    # create if missing
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    exists = cur.fetchone() is not None

    cols = headers + ["raw_json"]
    if not exists:
        col_defs = []
        for c in cols:
            if pk and c == pk:
                col_defs.append(f'"{c}" TEXT PRIMARY KEY')
            else:
                col_defs.append(f'"{c}" TEXT')
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(col_defs)})')
        conn.commit()
        return

    # alter missing columns
    cur.execute(f'PRAGMA table_info("{table}")')
    existing = {r[1] for r in cur.fetchall()}  # name at index 1
    for c in cols:
        if c not in existing:
            cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}" TEXT')
    conn.commit()


def _norm_key(k: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (k or "").strip().lower())

def _collect_fields(entry: Any) -> Dict[str, Any]:
    """Extract a best-effort {label/name -> value} map from a Memento entry detail."""
    if not isinstance(entry, dict):
        return {}

    # Unwrap common envelopes
    for wrap_key in ("entry", "data", "result"):
        v = entry.get(wrap_key)
        if isinstance(v, dict) and ("fields" in v or "values" in v or "fieldValues" in v):
            entry = v
            break

    candidates = []
    for k in ("fields", "values", "fieldValues", "field_values"):
        v = entry.get(k)
        if v is not None:
            candidates.append(v)

    out: Dict[str, Any] = {}

    def put(key: Any, val: Any):
        if key is None:
            return
        key_s = str(key)
        if key_s not in out:
            out[key_s] = val

    for cand in candidates:
        if isinstance(cand, dict):
            for kk, vv in cand.items():
                put(kk, vv)
        elif isinstance(cand, list):
            for it in cand:
                if isinstance(it, dict):
                    # try common shapes
                    key = it.get("name") or it.get("label") or it.get("field") or it.get("key") or it.get("title") or it.get("id")
                    val = it.get("value") if "value" in it else it.get("val") if "val" in it else it.get("content") if "content" in it else it.get("v")
                    if val is None and len(it) == 1:
                        # {"X": 123}
                        k0 = next(iter(it.keys()))
                        if k0 not in ("name","label","field","key","title","id"):
                            key = k0
                            val = it[k0]
                    put(key, val)

    # Also include a few top-level keys (id/created/modified) if present
    for k in ("id", "created", "modified"):
        if k in entry:
            put(k, entry.get(k))

    return out

def _entry_to_row(entry: Dict[str, Any], headers: List[str]) -> Dict[str, str]:
    fields_raw = _collect_fields(entry)
    fields_norm = {_norm_key(k): v for k, v in fields_raw.items()}

    out: Dict[str, str] = {}
    for h in headers:
        v = None
        # exact label match
        if h in fields_raw:
            v = fields_raw.get(h)
        else:
            hn = _norm_key(h)
            if hn in fields_norm:
                v = fields_norm.get(hn)
            elif isinstance(entry, dict) and h in entry:
                v = entry.get(h)
            elif isinstance(entry, dict) and hn == "extid" and "id" in entry:
                v = entry.get("id")
        out[h] = "" if v is None else str(v)

    out["raw_json"] = json.dumps(entry, ensure_ascii=False)
    return out


def _rows_match(csv_row: Dict[str, str], api_row: Dict[str, str], headers: List[str]) -> bool:
    # Normalize like CSV: trim, None->"", strip
    for h in headers:
        a = (api_row.get(h) or "").strip()
        b = (csv_row.get(h) or "").strip()
        if a != b:
            return False
    return True


def _autodetect_enrich_details(
    *,
    base_dir: str,
    ini_path: str,
    section: str,
    table: str,
    library_id: str,
    limit: int,
) -> Dict[str, Any]:
    """
    Decide (solo se possibile) se serve enrich_details per matchare il CSV.
    Ritorna dict: {decided: bool, enrich_details: bool, reason: str}
    """
    csv_zip = _find_csv_zip(base_dir)
    if not csv_zip:
        return {"decided": False, "enrich_details": False, "reason": "csv_zip_missing"}

    cache_path = os.path.join(base_dir, "enrich_autodetect.json")
    cache = _load_autodetect_cache(cache_path)

    csvinfo = _load_csv_header_and_sample(csv_zip, section, table, library_id)
    headers = csvinfo.get("headers") or []
    pk = csvinfo.get("pk") or ""
    sample_by_id = csvinfo.get("sample_by_id") or {}
    signature = csvinfo.get("signature") or ""

    if not headers:
        return {"decided": False, "enrich_details": False, "reason": "csv_empty_or_unmatched"}

    # cache hit
    c = cache.get(section)
    if isinstance(c, dict) and c.get("signature") == signature and isinstance(c.get("enrich_details"), bool):
        return {"decided": True, "enrich_details": bool(c["enrich_details"]), "reason": "cache"}

    # Quick probe: fetch 1-2 pages WITHOUT details
    probed = []
    try:
        for page_i, chunk in enumerate(fetch_incremental_pages(library_id, limit=limit, enrich_details=False), start=1):
            if isinstance(chunk, list):
                probed.extend([e for e in chunk if isinstance(e, dict)])
            if page_i >= 2:
                break
    except Exception as ex:
        # If probe fails, don't decide automatically
        return {"decided": False, "enrich_details": False, "reason": f"probe_failed:{str(ex)[:120]}"}

    if not probed:
        return {"decided": False, "enrich_details": False, "reason": "probe_empty"}

    # If list entries have no fields, it's a strong sign details are needed for CSV-like columns.
    first = probed[0]
    fields = first.get("fields") if isinstance(first, dict) else None
    if not isinstance(fields, dict) or not fields:
        decided = True
        enrich = True
        reason = "list_missing_fields"
    else:
        # If we can compare by id, do strict match on overlapping sample rows.
        decided = True
        enrich = False
        reason = "list_match"
        if pk and sample_by_id:
            ok = 0
            bad = 0
            for e in probed:
                rid = str(e.get("id") or e.get(pk) or "").strip()
                if not rid:
                    continue
                csv_row = sample_by_id.get(rid)
                if not csv_row:
                    continue
                api_row = _entry_to_row(e, headers)
                if _rows_match(csv_row, api_row, headers):
                    ok += 1
                else:
                    bad += 1
                if ok + bad >= 12:
                    break
            if bad > 0:
                enrich = True
                reason = "list_mismatch_sample"
            elif ok == 0:
                # No overlap -> fall back to coverage heuristic
                covered = sum(1 for h in headers if h in fields)
                ratio = covered / max(1, len(headers))
                if ratio < 0.9:
                    enrich = True
                    reason = f"low_field_coverage:{ratio:.2f}"
        else:
            # No PK -> heuristic coverage
            covered = sum(1 for h in headers if h in fields)
            ratio = covered / max(1, len(headers))
            if ratio < 0.9:
                enrich = True
                reason = f"low_field_coverage:{ratio:.2f}"

    # persist
    cache[section] = {
        "enrich_details": bool(enrich),
        "signature": signature,
        "decided_at": datetime.now().isoformat(timespec="seconds"),
        "reason": reason,
        "csv_member": csvinfo.get("member") or "",
    }
    _save_autodetect_cache(cache_path, cache)

    if ini_path:
        _persist_ini_enrich_details(ini_path, section, bool(enrich))

    return {"decided": True, "enrich_details": bool(enrich), "reason": reason}


def import_library_incremental(
    conn,
    table: str,
    library_id: str,
    limit: int = 100,
    enrich_details: bool = False,
    base_dir: str = "",
    ini_path: str = "",
    section: str = "",
):
    """
    Import incremental:
    - commit per pagina (no più tutto-o-niente)
    - schema = colonne CSV (se disponibili) + raw_json
    - autodetect enrich_details (solo primo run) se memento_all_csv.zip è presente
    """
    ensure_sync_state_table(conn)

    # Autodetect (solo se base_dir e ini_path disponibili)
    if base_dir and section:
        try:
            det = _autodetect_enrich_details(
                base_dir=base_dir,
                ini_path=ini_path,
                section=section,
                table=table,
                library_id=library_id,
                limit=limit,
            )
            if det.get("decided"):
                enrich_details = bool(det.get("enrich_details"))
                log(f"[autodetect] {section}: enrich_details={enrich_details} reason={det.get('reason')}")
        except Exception:
            pass

    # Load CSV header for schema (if present)
    headers: List[str] = []
    pk = ""
    csv_zip = _find_csv_zip(base_dir) if base_dir else ""
    if csv_zip:
        try:
            csvinfo = _load_csv_header_and_sample(csv_zip, section or table, table, library_id)
            headers = csvinfo.get("headers") or []
            pk = csvinfo.get("pk") or ""
        except Exception:
            headers = []
            pk = ""

    if not headers:
        # Fallback minimal schema
        headers = ["id", "modified"]

    _ensure_table_schema(conn, table, headers, pk)

    last_modified_remote = load_sync_state(conn, library_id)
    if last_modified_remote:
        log(f"Checkpoint precedente (last_modified_remote): {last_modified_remote}")
    else:
        log("Checkpoint precedente (last_modified_remote): — nessuno —")

    inserted = 0
    page_no = 0

    def _insert_chunk(chunk: List[Dict[str, Any]]):
        """Inserisce una pagina (chunk) in modo atomico.
        Se enrich_details=True e il mapping produce righe vuote (tutte colonne CSV vuote), interrompe subito.
        """
        cur = conn.cursor()
        cols = headers + ["raw_json"]
        placeholders = ", ".join(["?"] * len(cols))
        col_sql = ", ".join([f'"{c}"' for c in cols])
        sql = f'INSERT OR REPLACE INTO "{table}" ({col_sql}) VALUES ({placeholders})'

        conn.execute("SAVEPOINT memento_page")
        try:
            vals = []
            for e in chunk:
                row = _entry_to_row(e, headers)

                if enrich_details:
                    # Fail-fast: non ha senso continuare a scrivere righe vuote fino alla fine
                    has_any = False
                    for h in headers:
                        vv = row.get(h, "")
                        if vv is None:
                            vv = ""
                        if str(vv).strip() != "":
                            has_any = True
                            break
                    if not has_any:
                        raise RuntimeError("fields vuoti dopo enrichment")

                vals.append([row.get(c, "") for c in cols])

            cur.executemany(sql, vals)
            conn.execute("RELEASE SAVEPOINT memento_page")
        except Exception:
            conn.execute("ROLLBACK TO SAVEPOINT memento_page")
            conn.execute("RELEASE SAVEPOINT memento_page")
            raise

    t0 = time.time()
    for chunk in fetch_incremental_pages(
        library_id,
        modified_after_iso=last_modified_remote,
        limit=limit,
        enrich_details=enrich_details,
        progress=lambda ev: log(f"[incremental] Progress: {ev}") if isinstance(ev, dict) and "rows" in ev else None,
    ):
        if not chunk:
            continue
        page_no += 1
        _insert_chunk(chunk)
        conn.commit()
        inserted += len(chunk)

        # checkpoint after commit
        last_mod = None
        if isinstance(chunk[-1], dict):
            last_mod = chunk[-1].get("modified")
        if last_mod:
            save_sync_state(conn, library_id, last_mod)

        dt = time.time() - t0
        log(f"Pagina {page_no}: +{len(chunk)} righe (tot={inserted}) — {dt:.3f}s")

    log(f"Import completato: {inserted} righe totali")
    return inserted
# ---------------------------------------------------------------------
# Entry point batch
# ---------------------------------------------------------------------

def run_batch(db_path: str, batch_cfg: Dict[str, Dict[str, Any]], batch_path: str = ''):
    conn = sqlite3.connect(db_path)
    total_inserted = 0

    try:
        for section, cfg in batch_cfg.items():
            table = cfg["table"]
            library_id = cfg.get("library_id") or cfg.get("library")
            if not library_id:
                raise ValueError("library_id mancante")

            sync = cfg.get("sync", "incremental")
            limit = int(cfg.get("limit", 100))
            # Optional: allow disabling detail enrichment to avoid long waits.
            enrich_details = str(cfg.get("enrich_details", "true")).strip().lower() not in ("0","false","no","off")

            log(
                f"Sezione [{section}] → tabella '{table}', "
                f"libreria='{library_id}', sync='{sync}', limit={limit}"
            )

            if sync == "incremental":
                total_inserted += import_library_incremental(
                    conn,
                    table=table,
                    library_id=library_id,
                    limit=limit,
                    enrich_details=enrich_details,
                    base_dir=os.path.dirname(os.path.abspath(batch_path)) if batch_path else os.getcwd(),
                    ini_path=batch_path if batch_path and batch_path.lower().endswith('.ini') else '',
                    section=section,
                )
            else:
                log_error(f"Modalità sync non supportata: {sync}")

        return total_inserted
    except Exception as e:
        log_error(f"Import batch fallito: {e}")
        raise
    finally:
        conn.close()
# ---------------------------------------------------------------------
# Public helper: load batch from INI/YAML and run
# ---------------------------------------------------------------------

def _load_batch_cfg_from_ini(path: str) -> Dict[str, Dict[str, Any]]:
    import configparser

    cfgp = configparser.ConfigParser(interpolation=None)
    read_ok = cfgp.read(path, encoding="utf-8")
    if not read_ok:
        raise FileNotFoundError(f"Batch INI non trovato o illeggibile: {path}")

    out: Dict[str, Dict[str, Any]] = {}
    for section in cfgp.sections():
        d = dict(cfgp.items(section))
        # default: table = nome sezione
        d.setdefault("table", section)
        out[section] = d
    return out

def _load_batch_cfg_from_yaml(path: str) -> Dict[str, Dict[str, Any]]:
    try:
        import yaml  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "Batch YAML richiesto ma PyYAML non è installato. "
            "Installa con: pip install pyyaml"
        ) from e

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if isinstance(data, list):
        # supporta lista di oggetti {name:..., ...}
        out: Dict[str, Dict[str, Any]] = {}
        for i, item in enumerate(data, start=1):
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or item.get("section") or f"item{i}")
            out[name] = {k: v for k, v in item.items() if k not in ("name", "section")}
            out[name].setdefault("table", name)
        return out

    if not isinstance(data, dict):
        raise ValueError("Formato YAML batch non valido: atteso dict o list")

    out2: Dict[str, Dict[str, Any]] = {}
    for section, cfg in data.items():
        if not isinstance(cfg, dict):
            continue
        d = dict(cfg)
        d.setdefault("table", section)
        out2[str(section)] = d
    return out2

def memento_import_batch(db_path: str, batch_path: str):
    """
    Entry point compatibile con il menu:
    - batch_path può essere .ini / .yaml / .yml
    Ritorna il numero totale di righe importate.
    """
    batch_path = batch_path.strip()
    if batch_path.lower().endswith((".yaml", ".yml")):
        batch_cfg = _load_batch_cfg_from_yaml(batch_path)
    else:
        batch_cfg = _load_batch_cfg_from_ini(batch_path)

    return run_batch(db_path, batch_cfg, batch_path)

# v10
import os
import sys
import time
import sqlite3
from datetime import datetime
from typing import Any, Dict, List

from memento_sdk import (
    fetch_incremental,
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

def import_library_incremental(
    conn,
    table: str,
    library_id: str,
    limit: int,
):
    log(f"Import incremental → libreria={library_id} tabella={table}")

    checkpoint = load_sync_state(conn, library_id)
    log(
        "Checkpoint precedente (last_modified_remote): "
        + (checkpoint if checkpoint else "— nessuno —")
    )

    all_entries: List[Dict] = []

    def on_progress(ev):
        # ev può essere dict o list
        if isinstance(ev, dict):
            rows = ev.get("rows")
            page = ev.get("page")
            sec = ev.get("seconds")
            log(f"[incremental] Pagina {page} — {rows} righe in {sec:.3f}s")
        elif isinstance(ev, list):
            log(f"[incremental] Chunk grezzo: {len(ev)} elementi")
        else:
            log(f"[incremental] Progress non standard: {type(ev)}")

    try:
        raw = fetch_incremental(
            library_id=library_id,
            since=checkpoint,
            limit=limit,
            progress=on_progress,
        )
    except Exception as e:
        log_error(f"Fetch incremental fallito: {e}")
        raise

    entries = flatten_entries(raw)
    log(f"Fetch completato: {len(entries)} entry normalizzate")

    if not entries:
        log("Nessuna entry da importare.")
        return 0

    cur = conn.cursor()

    CHUNK = 200
    total = len(entries)
    inserted = 0

    for i in range(0, total, CHUNK):
        chunk = entries[i : i + CHUNK]
        t0 = time.time()

        for e in chunk:
            # esempio minimale: adattalo al tuo schema reale
            cur.execute(
                f"""
                INSERT OR IGNORE INTO {table}
                (remote_id, modified_remote, payload_json)
                VALUES (?, ?, ?)
                """,
                (
                    e.get("id"),
                    e.get("modified"),
                    str(e),
                ),
            )

        conn.commit()
        inserted += len(chunk)
        dt = time.time() - t0

        last_mod = chunk[-1].get("modified")
        if last_mod:
            save_sync_state(conn, library_id, last_mod)

        log(
            f"Chunk {i//CHUNK + 1}: "
            f"{inserted}/{total} righe — {dt:.3f}s"
        )

    log(f"Import completato: {inserted} righe totali")


    return inserted
# ---------------------------------------------------------------------
# Entry point batch
# ---------------------------------------------------------------------

def run_batch(db_path: str, batch_cfg: Dict[str, Dict[str, Any]]):
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

    return run_batch(db_path, batch_cfg)

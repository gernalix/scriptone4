# v9
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
        return

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

# ---------------------------------------------------------------------
# Entry point batch
# ---------------------------------------------------------------------

def run_batch(db_path: str, batch_cfg: Dict[str, Dict[str, Any]]):
    conn = sqlite3.connect(db_path)

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
                import_library_incremental(
                    conn,
                    table=table,
                    library_id=library_id,
                    limit=limit,
                )
            else:
                log_error(f"Modalità sync non supportata: {sync}")

    except Exception as e:
        log_error(f"Import batch fallito: {e}")
        raise
    finally:
        conn.close()

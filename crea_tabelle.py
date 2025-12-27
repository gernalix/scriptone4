# v1
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, sys, re, json, sqlite3
from memento_import import memento_import_batch as memento_import_batch_ini
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date
from zoneinfo import ZoneInfo

__CREA_TABELLE_VERSION__ = "v15.1"
DEFAULT_DB_PATH = os.environ.get("CREA_TABELLE_DB", "noutput.db")
AUDIT_DDL="audit_schema"
AUDIT_DML="audit_dml"

# ------------------------- deps opzionali -------------------------
try:
    from pymementodb import Memento
    _HAS_PYMEMENTO=True
except Exception:
    _HAS_PYMEMENTO=False

try:
    import yaml
    _HAS_YAML=True
except Exception:
    _HAS_YAML=False

# ------------------------- util -------------------------
def _short(s,n=200):
    if s is None: return ""
    s=str(s).replace("\n"," ").replace("\r"," ")
    return (s[:n]+"...") if len(s)>n else s

def _json_default(o):
    if isinstance(o,(datetime,date)): return o.isoformat()
    return str(o)

def _parse_dt_any(v):
    """Ritorna datetime aware; assume UTC se naive."""
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    elif isinstance(v, date):
        dt = datetime.fromisoformat(v.isoformat())
    else:
        s = str(v).strip().replace("T", " ").replace("Z", "+00:00")
        # Supporta frazioni secondi e offset ISO
        dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt

def _iso_in_policy(v, tz_name: str | None, store_time_as: str = "utc"):
    """
    Converte un valore temporale generico in stringa ISO in base a policy.
    store_time_as: 'utc' | 'local'
    tz_name: es. 'Europe/Copenhagen' (usato solo se store_time_as=local)
    """
    dt = _parse_dt_any(v)
    if dt is None:
        return None
    if (store_time_as or "utc").lower() == "local" and tz_name:
        try:
            dt = dt.astimezone(ZoneInfo(tz_name))
        except Exception:
            # Fallback: resta UTC
            dt = dt.astimezone(ZoneInfo("UTC"))
    else:
        dt = dt.astimezone(ZoneInfo("UTC"))
    return dt.isoformat(timespec="seconds")

def read_memento_config(path="memento_import.yaml"):
    cfg={}
    if not _HAS_YAML or not os.path.exists(path): return cfg
    try:
        with open(path,"r",encoding="utf-8") as f:
            d=yaml.safe_load(f) or {}
            if isinstance(d,dict): cfg=d
    except Exception: pass
    return cfg

def db_connect(p):
    c=sqlite3.connect(p); c.row_factory=sqlite3.Row; return c

def quote_ident(n:str)->str:
    return ('"' + n.replace('"','""') + '"' ) if re.search(r"[^A-Za-z0-9_]",n) else n

def ensure_audit_tables(conn):
    conn.execute(f"""CREATE TABLE IF NOT EXISTS {quote_ident(AUDIT_DDL)} (
        id INTEGER PRIMARY KEY,
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        action TEXT,
        object_type TEXT,
        object_name TEXT,
        details TEXT
    )""")
    conn.execute(f"""CREATE TABLE IF NOT EXISTS {quote_ident(AUDIT_DML)} (
        id INTEGER PRIMARY KEY,
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        table_name TEXT,
        op TEXT,
        row_id INTEGER,
        details TEXT
    )""")
    conn.commit()

def _table_columns(conn,table):
    try:
        return [r[1] for r in conn.execute(f"PRAGMA table_info({quote_ident(table)});").fetchall()]
    except sqlite3.Error:
        return []

def _first_present_name(pref, avail):
    low={c.lower():c for c in avail}
    for p in pref:
        if p.lower() in low: return low[p.lower()]
    return None

def audit_schema(conn, action, object_type, object_name, details=None):
    cols=_table_columns(conn,AUDIT_DDL)
    if not cols: return
    vm={}
    a=_first_present_name(["action","azione"],cols)
    t=_first_present_name(["object_type","oggetto_tipo","tipo"],cols)
    n=_first_present_name(["object_name","oggetto_nome","nome"],cols)
    d=_first_present_name(["details","dettaglio","dettagli","extra"],cols)
    if a: vm[a]=action
    if t: vm[t]=object_type
    if n: vm[n]=object_name
    if d: vm[d]= json.dumps(details,ensure_ascii=False,default=_json_default) if isinstance(details,dict) else ("" if details is None else str(details))
    if not vm: return
    cols_sql=", ".join(quote_ident(k) for k in vm.keys()); ph=", ".join(["?"]*len(vm))
    try:
        conn.execute(f"INSERT INTO {quote_ident(AUDIT_DDL)} ({cols_sql}) VALUES ({ph})", list(vm.values()))
        conn.commit()
    except sqlite3.Error as e:
        print(f"[diag] audit_schema insert skip: {e}")

# ------------------------- Memento SDK helpers -------------------------
def sdk_connect(token):
    if not _HAS_PYMEMENTO:
        print("[ERRORE] Installa pymementodb: pip install pymementodb"); return None
    try:
        return Memento(token)
    except Exception as e:
        print(f"[ERRORE] SDK: {type(e).__name__}: {_short(e)}"); return None

def sdk_list_libraries(srv):
    try:
        libs=srv.list_libraries() or []
        out=[]
        for lib in libs:
            out.append(lib if isinstance(lib,dict) else getattr(lib,"__dict__",{"name":str(lib)}))
        return out
    except Exception as e:
        print(f"[ERRORE] list_libraries: {type(e).__name__}: {_short(e)}"); return []

def sdk_get_entries(srv, lib_id, limit=None, offset=None):
    try:
        if limit is None and offset is None:
            entries=srv.get_entries(lib_id) or []
        elif offset is None:
            entries=srv.get_entries(lib_id, limit=limit or 1000) or []
        else:
            entries=srv.get_entries(lib_id, limit=limit or 1000, offset=offset) or []
        return [ getattr(e,"__dict__",e) for e in entries ]
    except Exception as e:
        print(f"[diag] get_entries: {type(e).__name__}: {_short(e)}"); return []

# ------------------------- Mapping euristico -------------------------
def detect_datetime_like(v):
    if isinstance(v,(datetime,date)): return True
    if isinstance(v,str) and re.match(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}", v.strip()): return True
    return False

def detect_small_int(v):
    if isinstance(v,bool): return False
    if isinstance(v,int): return 0<=v<=20
    if isinstance(v,str) and v.strip().lstrip("-").isdigit():
        try:
            x=int(v.strip())
            return 0<=x<=20
        except Exception:
            return False
    return False

def deduce_mapping_from_entries(entries):
    vt,vm,vn={}, {}, {}
    for e in entries[:min(len(entries),50)]:
        fields=e.get("fields") or e.get("values") or e.get("data") or []
        if isinstance(fields,list):
            for it in fields:
                if not isinstance(it,dict): continue
                fid=it.get("id"); val=it.get("value")
                if fid is None: continue
                if detect_datetime_like(val): vt[fid]=vt.get(fid,0)+1
                elif detect_small_int(val): vm[fid]=vm.get(fid,0)+1
                elif isinstance(val,str) and val.strip(): vn[fid]=vn.get(fid,0)+1
        elif isinstance(fields,dict):
            for k,v in fields.items():
                if detect_datetime_like(v): vt[k]=vt.get(k,0)+1
                elif detect_small_int(v): vm[k]=vm.get(k,0)+1
                elif isinstance(v,str) and v.strip(): vn[k]=vn.get(k,0)+1
    def top(d):
        return None if not d else sorted(d.items(), key=lambda kv:(-kv[1], str(kv[0])))[0][0]
    return {"tempo_id": top(vt), "umore_id": top(vm), "note_id": top(vn)}

# ------------------------- DDL & import -------------------------
def ensure_import_table(conn, table, id_mode, tempo_col):
    if _table_columns(conn,table): return
    if id_mode=="surrogate":
        ddl=f"""
            CREATE TABLE {quote_ident(table)} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ext_id TEXT UNIQUE NOT NULL,
                {quote_ident(tempo_col)} TEXT,
                second_time TEXT,
                umore INTEGER,
                note TEXT,
                createdTime TEXT,
                modifiedTime TEXT,
                raw JSON
            )
        """
    else:
        ddl=f"""
            CREATE TABLE {quote_ident(table)} (
                id TEXT PRIMARY KEY,
                {quote_ident(tempo_col)} TEXT,
                second_time TEXT,
                umore INTEGER,
                note TEXT,
                ext_id TEXT UNIQUE,
                createdTime TEXT,
                modifiedTime TEXT,
                raw JSON
            )
        """
    conn.executescript(ddl); conn.commit()

def create_time_indexes(conn, table, tempo_col, create_second):
    try:
        conn.execute(f"CREATE INDEX IF NOT EXISTS {quote_ident('idx_'+table+'_'+tempo_col)} ON {quote_ident(table)} ({quote_ident(tempo_col)})")
    except sqlite3.Error as e:
        print(f"[diag] idx primary skip: {e}")
    if create_second:
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS {quote_ident('idx_'+table+'_second_time')} ON {quote_ident(table)} (second_time)")
        except sqlite3.Error as e:
            print(f"[diag] idx second skip: {e}")
    conn.commit()

def import_entries(conn, table, entries, mapping, id_mode, tempo_col, prefer_time, tz_name, store_time_as):
    ins=upd=0; second_used=None
    tempo_id=mapping.get("tempo_id"); umore_id=mapping.get("umore_id"); note_id=mapping.get("note_id")
    for e in entries:
        ext_id=e.get("id")
        if not ext_id: continue
        fields=e.get("fields") or e.get("values") or e.get("data") or []
        fdict={}
        if isinstance(fields,list):
            for it in fields:
                if isinstance(it,dict) and "id" in it: fdict[it["id"]]=it.get("value")
        elif isinstance(fields,dict): fdict=dict(fields)
        field_time=fdict.get(tempo_id) if tempo_id is not None else None
        created=e.get("createdTime"); modified=e.get("modifiedTime")
        # selezione primario/secondario
        if prefer_time=="field" and field_time is not None:
            pt=field_time; st=created or modified
        elif prefer_time=="created" and created is not None:
            pt=created; st=field_time or modified
        elif prefer_time=="modified" and modified is not None:
            pt=modified; st=field_time or created
        else:
            pt=field_time or created or modified
            st= next((c for c in [created, modified, field_time] if c is not None and c!=pt), None)
        tempo_val   = _iso_in_policy(pt, tz_name, store_time_as) if pt is not None else None
        second_val  = _iso_in_policy(st, tz_name, store_time_as) if st is not None else None
        if second_val is not None: second_used="second_time"
        # umore
        uv=None
        if umore_id is not None:
            v=fdict.get(umore_id)
            if isinstance(v,bool): uv=int(v)
            elif isinstance(v,int): uv=v
            elif isinstance(v,str) and v.strip().lstrip("-").isdigit():
                try: uv=int(v.strip())
                except: uv=None
        # note
        nv=None
        if note_id is not None:
            v=fdict.get(note_id)
            nv=v.isoformat() if isinstance(v,(datetime,date)) else (str(v) if v is not None else None)
        # meta
        created_str = _iso_in_policy(created, tz_name, store_time_as) if created is not None else None
        modified_str= _iso_in_policy(modified, tz_name, store_time_as) if modified is not None else None
        raw_json = json.dumps(e, ensure_ascii=False, default=_json_default)

        if id_mode=="surrogate":
            row=conn.execute(f"SELECT ext_id FROM {quote_ident(table)} WHERE ext_id=?", (ext_id,)).fetchone()
            if row:
                conn.execute(f"UPDATE {quote_ident(table)} SET {quote_ident(tempo_col)}=?, second_time=?, umore=?, note=?, createdTime=?, modifiedTime=?, raw=? WHERE ext_id=?",
                             (tempo_val, second_val, uv, nv, created_str, modified_str, raw_json, ext_id))
                upd+=1
            else:
                conn.execute(f"INSERT INTO {quote_ident(table)} (ext_id, {quote_ident(tempo_col)}, second_time, umore, note, createdTime, modifiedTime, raw) VALUES (?,?,?,?,?,?,?,?)",
                             (ext_id, tempo_val, second_val, uv, nv, created_str, modified_str, raw_json))
                ins+=1
        else:
            row=conn.execute(f"SELECT id FROM {quote_ident(table)} WHERE id=?", (ext_id,)).fetchone()
            if row:
                conn.execute(f"UPDATE {quote_ident(table)} SET {quote_ident(tempo_col)}=?, second_time=?, umore=?, note=?, ext_id=?, createdTime=?, modifiedTime=?, raw=? WHERE id=?",
                             (tempo_val, second_val, uv, nv, ext_id, created_str, modified_str, raw_json, ext_id))
                upd+=1
            else:
                conn.execute(f"INSERT INTO {quote_ident(table)} (id, {quote_ident(tempo_col)}, second_time, umore, note, ext_id, createdTime, modifiedTime, raw) VALUES (?,?,?,?,?,?,?,?,?)",
                             (ext_id, tempo_val, second_val, uv, nv, ext_id, created_str, modified_str, raw_json))
                ins+=1
    conn.commit()
    return ins, upd, tempo_col, second_used

# ------------------------- Flow Memento -------------------------
def memento_elenca_librerie():
    print("\n================ MEMENTO — Elenco librerie (SDK) ================")
    cfg=read_memento_config(); token=(cfg.get("token") or "").strip()
    if not token: token=input("Token API: ").strip()
    srv=sdk_connect(token)
    if not srv: return
    libs=sdk_list_libraries(srv)
    if not libs:
        print("[ERRORE] Nessuna libreria visibile."); return
    print(f"[info] Librerie (totali: {len(libs)}):")
    for i,lib in enumerate(libs,1):
        lid=str(lib.get("id") or lib.get("uid") or lib.get("uuid") or "?"); name=str(lib.get("name") or "?")
        print(f"  {i:>3}) id={lid} | name={name}")

def memento_deduci_mappatura():
    print("\n================ MEMENTO — Deduci mappatura campi ================")
    cfg=read_memento_config(); token=(cfg.get("token") or "").strip(); lib_id=(cfg.get("library_id") or "").strip()
    if not token: token=input("Token API: ").strip()
    else: print("[cfg] token: *** (da YAML)")
    if not lib_id: lib_id=input("Library ID: ").strip()
    else: print(f"[cfg] library_id: {lib_id}")
    srv=sdk_connect(token)
    if not srv: return
    entries=sdk_get_entries(srv, lib_id, limit=50)
    if not entries:
        print("[ERRORE] Nessuna entry visibile o permessi insufficienti."); return
    m=deduce_mapping_from_entries(entries)
    print("[OK] Mappatura dedotta:")
    print(f"  tempo_id = {m.get('tempo_id')} (se None userò created/modified)")
    print(f"  umore_id = {m.get('umore_id')}")
    print(f"  note_id  = {m.get('note_id')}")

def memento_mostra_entry():
    print("\n================ MEMENTO — Mostra 1 entry grezza ================")
    cfg=read_memento_config(); token=(cfg.get("token") or "").strip(); lib_id=(cfg.get("library_id") or "").strip()
    if not token: token=input("Token API: ").strip()
    else: print("[cfg] token: *** (da YAML)")
    if not lib_id: lib_id=input("Library ID: ").strip()
    else: print(f"[cfg] library_id: {lib_id}")
    srv=sdk_connect(token)
    if not srv: return
    entries=sdk_get_entries(srv, lib_id, limit=1)
    if not entries:
        print("[info] Nessuna entry visibile o permessi insufficienti."); return
    try:
        print(json.dumps(entries[0], ensure_ascii=False, indent=2, default=_json_default))
    except Exception:
        print(entries[0])

def _do_one_import(conn, srv, lib_id, table, id_mode, prefer_time, time_column_name, index_second_time, tz_name, store_time_as):
    id_mode=(id_mode or "external").lower()
    if id_mode not in ("external","surrogate"): id_mode="external"
    prefer_time=(prefer_time or "auto").lower()
    if prefer_time not in ("auto","field","created","modified"): prefer_time="auto"
    tempo_col=time_column_name or "tempo"
    sample=sdk_get_entries(srv, lib_id, limit=100)
    if not sample:
        print(f"[ERRORE] Nessuna entry visibile per {lib_id}."); return (0,0)
    mapping=deduce_mapping_from_entries(sample)
    ensure_import_table(conn, table, id_mode, tempo_col)
    entries=sdk_get_entries(srv, lib_id) or sample
    ins, upd, primary_used, second_used = import_entries(conn, table, entries, mapping, id_mode, tempo_col, prefer_time, tz_name, store_time_as)
    create_time_indexes(conn, table, tempo_col, bool(index_second_time and second_used))
    audit_schema(conn, "CLOUD_IMPORT", "table", table, f"importate {ins} righe da memento per la libreria {lib_id}")
    print(f"[OK] {lib_id}: inserite {ins}, aggiornate {upd}. Tempo primario: {primary_used}{' + second_time' if second_used else ''}")
    return (ins, upd)

def memento_import_singolo(conn):
    print("\n================ MEMENTO — Importa libreria (auto) ================")
    cfg=read_memento_config(); token=(cfg.get("token") or "").strip(); lib_id=(cfg.get("library_id") or "").strip()
    default=cfg.get("default",{}) if isinstance(cfg.get("default"),dict) else {}
    id_mode=default.get("id_mode") or "external"
    prefer_time=default.get("prefer_time") or "auto"
    time_column_name=default.get("time_column_name") or "tempo"
    index_second_time=bool(default.get("index_second_time", False))
    tz_name=default.get("timezone") or None
    store_time_as=(default.get("store_time_as") or "utc").lower()
    if not token: token=input("Token API: ").strip()
    else: print("[cfg] token: *** (da YAML)")
    if not lib_id: lib_id=input("Library ID: ").strip()
    else: print(f"[cfg] library_id: {lib_id}")
    table=input(f"Nome tabella destinazione [memento_{lib_id}]: ").strip() or f"memento_{lib_id}"
    srv=sdk_connect(token)
    if not srv: return
    _do_one_import(conn, srv, lib_id, table, id_mode, prefer_time, time_column_name, index_second_time, tz_name, store_time_as)

def memento_import_batch(conn):
    print("\n================ MEMENTO — Import batch da YAML ================")
    cfg=read_memento_config(); token=(cfg.get("token") or "").strip()
    default=cfg.get("default",{}) if isinstance(cfg.get("default"),dict) else {}
    id_mode_def=default.get("id_mode") or "external"
    prefer_time_def=default.get("prefer_time") or "auto"
    time_column_name_def=default.get("time_column_name") or "tempo"
    index_second_time_def=bool(default.get("index_second_time", False))
    tz_name_def=default.get("timezone") or None
    store_time_as_def=(default.get("store_time_as") or "utc").lower()
    libs_cfg=cfg.get("libraries") if isinstance(cfg,dict) else None
    if not token:
        print("[ERRORE] Manca token in YAML (chiave: token)."); return
    if not libs_cfg or not isinstance(libs_cfg, list):
        print("[ERRORE] Manca lista 'libraries' in YAML. Esempio:")
        print("""\
token: TUO_TOKEN
default:
  id_mode: external
  prefer_time: auto
  time_column_name: tempo
  index_second_time: false
  timezone: Europe/Copenhagen
  store_time_as: local
libraries:
  - id: wn8JsQymR
    table: umore
    id_mode: surrogate
    prefer_time: field
    time_column_name: tempo
    index_second_time: true
  - id: r7JvCKmKc
    table: ansia
""".strip()); return
    srv=sdk_connect(token)
    if not srv: return
    total_ins=total_upd=0
    for item in libs_cfg:
        if not isinstance(item,dict): continue
        lib_id=str(item.get("id") or "").strip()
        if not lib_id:
            print("[skip] elemento senza id"); continue
        table=str(item.get("table") or f"memento_{lib_id}").strip()
        id_mode=item.get("id_mode") or id_mode_def
        prefer_time=item.get("prefer_time") or prefer_time_def
        time_column_name=item.get("time_column_name") or time_column_name_def
        index_second_time=bool(item.get("index_second_time", index_second_time_def))
        tz_name=item.get("timezone") or tz_name_def
        store_time_as=(item.get("store_time_as") or store_time_as_def).lower()
        ins, upd = _do_one_import(conn, srv, lib_id, table, id_mode, prefer_time, time_column_name, index_second_time, tz_name, store_time_as)
        total_ins+=ins; total_upd+=upd
    print(f"[OK] Batch completato. Totale inserite {total_ins}, aggiornate {total_upd}.")

# ------------------------- Flussi SQLite -------------------------
def flow_add_table(conn):
    print("\n================ Aggiungi tabella ================")
    name=input("Nome nuova tabella: ").strip()
    if not name: print("Nome non valido."); return
    if name in (AUDIT_DDL,AUDIT_DML): print("Nome riservato."); return
    try:
        conn.execute(f"CREATE TABLE {quote_ident(name)} (id INTEGER PRIMARY KEY)"); conn.commit()
        audit_schema(conn,"ADD_TABLE","table",name,"creata tabella vuota con PK id")
        print(f"[OK] Tabella '{name}' creata.")
    except sqlite3.Error as e:
        print(f"[ERRORE] Creazione tabella fallita: {e}")

def flow_edit_table(conn):
    print("\n================ Modifica tabella ================")
    tables=[r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()]
    if not tables: print("Nessuna tabella."); return
    for i,t in enumerate(tables,1): print(f"  {i}) {t}")
    try:
        idx=int(input("> Numero tabella: ").strip()); table=tables[idx-1]
    except Exception:
        print("Scelta non valida."); return
    while True:
        print(f"\n--- Modifica '{table}' ---"); print("1) Elenca colonne"); print("2) Aggiungi colonna"); print("3) Elimina tabella (DANGER)"); print("0) Indietro")
        sel=input("> ").strip()
        if sel=="1":
            for r in conn.execute(f"PRAGMA table_info({quote_ident(table)});").fetchall():
                print(f" - {r['name']} | {r['type']} | pk={r['pk']} | notnull={r['notnull']}")
        elif sel=="2":
            col=input("Nome colonna: ").strip(); ctype=input("Tipo (es. TEXT, INTEGER, REAL): ").strip() or "TEXT"
            try:
                conn.execute(f"ALTER TABLE {quote_ident(table)} ADD COLUMN {quote_ident(col)} {ctype}"); conn.commit()
                audit_schema(conn,"ADD_COLUMNS","table",table,f"aggiunta colonna {col} {ctype}"); print("[OK] Colonna aggiunta.")
            except sqlite3.Error as e:
                print(f"[ERRORE] ALTER TABLE fallito: {e}")
        elif sel=="3":
            if input("Confermi ELIMINA TABELLA? digita 'SI': ").strip()=="SI":
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {quote_ident(table)}"); conn.commit();
                    audit_schema(conn,"ELIMINA_TABELLA","table",table,"tabella eliminata"); print(f"[OK] Tabella '{table}' eliminata."); break
                except sqlite3.Error as e:
                    print(f"[ERRORE] DROP TABLE fallito: {e}")
            else:
                print("Annullato.")
        elif sel=="0": break
        else: print("Scelta non valida.")

def flow_rename_table(conn):
    print("\n================ Rinomina tabella ================")
    old=input("Nome tabella attuale: ").strip(); new=input("Nuovo nome: ").strip()
    if not old or not new: print("Nomi non validi."); return
    try:
        conn.execute(f"ALTER TABLE {quote_ident(old)} RENAME TO {quote_ident(new)}"); conn.commit()
        audit_schema(conn,"RENAME_TABLE","table",new,f"rinominata da {old} a {new}"); print(f"[OK] Rinomata '{old}' → '{new}'")
    except sqlite3.Error as e:
        print(f"[ERRORE] Rinomina fallita: {e}")

def flow_create_index(conn):
    print("\n================ Crea indice su colonna tempo ================")
    table=input("Tabella: ").strip(); col=input("Colonna tempo (es. tempo): ").strip() or "tempo"
    try:
        idx=f"idx_{table}_{col}"; conn.execute(f"CREATE INDEX IF NOT EXISTS {quote_ident(idx)} ON {quote_ident(table)} ({quote_ident(col)})"); conn.commit()
        audit_schema(conn,"CREATE_INDEX","index",idx,f"creato indice su {table}({col})"); print(f"[OK] Creato indice {idx} su {table}({col}).")
    except sqlite3.Error as e:
        print(f"[ERRORE] Creazione indice fallita: {e}")

# ------------------------- Main menu -------------------------
def main():
    print("="*64); print(f"CREA/MODIFICA TABELLE + TRIGGER — versione {__CREA_TABELLE_VERSION__}"); print("="*64)
    print(f"[crea_tabelle] python: {sys.version.split()[0]} — exe: {sys.executable}\n")
    db_path=input(f"Percorso DB [{DEFAULT_DB_PATH}]: ").strip() or DEFAULT_DB_PATH
    db_dir=os.path.dirname(db_path) or "."
    if not os.path.isdir(db_dir): print(f"[ERRORE] Directory inesistente: {db_dir}"); return
    try:
        conn=db_connect(db_path); ensure_audit_tables(conn); print(f"[INFO] Connessione OK: {os.path.abspath(db_path)}")
    except Exception as e:
        print(f"[ERRORE] Connessione al DB fallita: {e}"); return
    while True:
        print("\n================ MENU PRINCIPALE ================")
        print("1) Memento ▶")
        print("2) Aggiungi tabella (SQLite)")
        print("3) Modifica tabella (SQLite)")
        print("4) Rinomina tabella (SQLite)")
        print("5) Crea indice su colonna tempo (SQLite)")
        print("0) Esci")
        sel=input("> ").strip()
        if sel=="1":
            while True:
                print("\n------ MEMENTO -------")
                print("1) Elenca librerie (SDK)")
                print("2) Deduci mappatura campi (SDK)")
                print("3) Mostra 1 entry grezza (SDK)")
                print("4) Importa libreria (auto)")
                print("5) Importa batch da YAML")
                print("0) Indietro")
                msel=input("> ").strip()
                if msel=="1": memento_elenca_librerie()
                elif msel=="2": memento_deduci_mappatura()
                elif msel=="3": memento_mostra_entry()
                elif msel=="4": memento_import_singolo(conn)
                elif msel=="5":
                    batch_path = input("Percorso batch YAML/INI [memento_import.ini]: ").strip() or "memento_import.ini"
                    # usa l'implementazione nuova (INI/YAML + sync incremental)
                    try:
                        n = memento_import_batch_ini(db_path, batch_path)
                        print(f"\n[INFO] Righe importate: {n}")
                    except Exception as e:
                        print(f"\n[ERRORE] Import batch fallito: {e}")

                elif msel=="0": break
                else: print("Scelta non valida.")
        elif sel=="2": flow_add_table(conn)
        elif sel=="3": flow_edit_table(conn)
        elif sel=="4": flow_rename_table(conn)
        elif sel=="5": flow_create_index(conn)
        elif sel=="0": print("Ciao!"); break
        else: print("Scelta non valida.")

if __name__=="__main__":
    main()
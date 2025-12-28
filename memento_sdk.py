# v9
import os
import re
import json
import time
import requests
import socket
from typing import Dict, Any, List, Optional

def _get_with_backoff(url, *, params=None, timeout=None, max_tries=8, base_sleep=0.8, max_sleep=20.0):
    import random
    tries = 0
    r = None
    last_exc = None
    while tries < max_tries:
        try:
            # Redact token from logs
            _p = dict(params or {})
            if "token" in _p:
                _p["token"] = "***"
            if _debug_http():
                _log(f"SDK → GET {url} params={_p} try={tries+1}/{max_tries}")
            r = requests.get(url, params=params or {}, timeout=timeout or _timeout())
            if r.status_code < 400 or r.status_code in (400,401,403,404):
                return r
            if r.status_code == 429 or 500 <= r.status_code < 600:
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except Exception:
                        sleep_s = None
                else:
                    sleep_s = min(max_sleep, base_sleep * (2 ** tries)) + random.uniform(0, 0.4)
                time.sleep(sleep_s or 1.0)
                tries += 1
                last_exc = None
                continue
            return r
        except requests.RequestException as ex:
            last_exc = ex
            time.sleep(min(max_sleep, base_sleep * (2 ** tries)))
            tries += 1
            continue
    if r is None and last_exc:
        raise last_exc
    return r

DEFAULT_DB_PATH = r"Z:\download\datasette5\scriptone\noutput.db"

def get_default_db_path():
    return DEFAULT_DB_PATH

CFG = {}
try:
    import config as _user_config
    CFG = getattr(_user_config, "CONFIG", getattr(_user_config, "CFG", {}))
except Exception:
    pass

def _load_local_cfg():
    cfg = {}
    search_dirs = [
        os.getcwd(),
        os.path.dirname(os.path.abspath(__file__)),
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ]
    ini_paths = [os.path.join(d, "settings.ini") for d in search_dirs]
    yml_paths = [os.path.join(d, "settings.yaml") for d in search_dirs] + [os.path.join(d, "settings.yml") for d in search_dirs]

    for p in yml_paths:
        if os.path.exists(p):
            try:
                import yaml  # type: ignore
            except Exception:
                yaml = None
            if yaml:
                try:
                    with open(p, "r", encoding="utf-8") as fh:
                        y = yaml.safe_load(fh) or {}
                    if isinstance(y, dict):
                        for sect, d in y.items():
                            if isinstance(d, dict):
                                for k, v in d.items():
                                    cfg[f"{sect}.{k}"] = v
                except Exception:
                    pass

    for p in ini_paths:
        if os.path.exists(p):
            import configparser
            cp = configparser.ConfigParser()
            try:
                cp.read(p, encoding="utf-8")
                for sect in cp.sections():
                    for k, v in cp.items(sect):
                        cfg[f"{sect}.{k}"] = v
            except Exception:
                pass

    return cfg

def _cfg_get(key: str, default=None):
    val = None
    if isinstance(CFG, dict):
        val = CFG.get(key)

    if val is None:
        local = _load_local_cfg()
        val = local.get(key)

    if val is None:
        env_map = {
            "memento.token": "MEMENTO_TOKEN",
            "memento.api_url": "MEMENTO_API_URL",
            "memento.timeout": "MEMENTO_TIMEOUT"
        }
        env_key = env_map.get(key)
        if env_key:
            val = os.environ.get(env_key, None)

    return val if val is not None else default

def _sanitize_url(url: str) -> str:
    if not url:
        return url
    url = re.split(r"[;#]", str(url), maxsplit=1)[0]
    url = url.strip().strip('"').strip("'").strip()
    url = re.sub(r"\s+", "", url)
    return url

def _base_url() -> str:
    u = _cfg_get("memento.api_url", "https://api.mementodatabase.com/v1")
    return _sanitize_url(u or "" )

def _ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def _log(msg: str):
    try:
        print(f"[{_ts()}] {msg}")
    except Exception:
        # Never fail due to logging
        pass



def _debug_http() -> bool:
    v = str(os.environ.get("MEMENTO_DEBUG_HTTP", "0")).strip().lower()
    return v in ("1","true","yes","on")
def _timeout():
    """Return requests timeout.
    Supports:
      - memento.connect_timeout / env MEMENTO_CONNECT_TIMEOUT
      - memento.read_timeout    / env MEMENTO_READ_TIMEOUT
      - legacy memento.timeout  / env MEMENTO_TIMEOUT (read timeout)
    """
    def _to_int(val, default):
        try:
            return int(val)
        except Exception:
            return default

    legacy_read = _to_int(_cfg_get("memento.timeout", 20), 20)
    connect = _to_int(_cfg_get("memento.connect_timeout", min(10, legacy_read)), min(10, legacy_read))
    read = _to_int(_cfg_get("memento.read_timeout", legacy_read), legacy_read)

    # Apply a global socket timeout as an extra guard (Windows DNS/TLS stalls)
    try:
        socket.setdefaulttimeout(max(connect, read) + 5)
    except Exception:
        pass

    return (connect, read)

def _token_params() -> Dict[str, Any]:
    token = _cfg_get("memento.token", "").strip()
    return {"token": token} if token else {}

def _raise_on_404(r: requests.Response, path: str):
    if r.status_code == 404:
        raise RuntimeError(f"Memento API ha risposto 404 su {path}. URL: {r.request.method} {r.url}\nStatus: {r.status_code}\nBody: {r.text}")
    r.raise_for_status()



def _ensure_ok(resp, context: str):
    try:
        code = int(getattr(resp, "status_code", 0))
    except Exception:
        code = 0
    if code >= 400:
        txt = ""
        try:
            txt = getattr(resp, "text", "")
        except Exception:
            txt = ""
        txt = (txt or "").replace("\n", " ")
        raise RuntimeError(f"HTTP {code} during {context}: {txt[:200]}")
    return resp
def list_libraries():
    url = f"{_base_url().rstrip('/')}/libraries"
    r = _get_with_backoff(url, params=_token_params(), timeout=_timeout())
    _raise_on_404(r, "/libraries")
    try:
        data = r.json()
    except Exception:
        txt = r.text or ""
        try:
            data = json.loads(txt)
        except Exception:
            return [{"id": "raw", "name": txt.strip(), "title": txt.strip()}]

    items = None
    if isinstance(data, dict):
        items = data.get("libraries") or data.get("items") or data.get("data")
    if items is None:
        items = data
    if isinstance(items, dict):
        items = list(items.values())
    out = []
    for it in items or []:
        out.append({
            "id": it.get("id") or it.get("library_id") or it.get("uuid") or it.get("uid"),
            "name": it.get("name") or it.get("title") or it.get("label") or "",
            "title": it.get("title") or it.get("name") or ""
        })
    return out

def fetch_all_entries_full(library_id, limit=100, *, progress=None):
    import urllib.parse as _up
    base = _base_url().rstrip("/")
    url = f"{base}/libraries/{library_id}/entries"
    params = _token_params().copy()
    params["limit"] = int(limit)

    def _listify(data):
        if isinstance(data, dict):
            items = data.get("entries") or data.get("items") or data.get("data")
        else:
            items = data
        if isinstance(items, dict):
            vals = list(items.values())
            if len(vals) == 1 and isinstance(vals[0], list):
                items = vals[0]
            else:
                flat = []
                for v in vals:
                    if isinstance(v, list):
                        flat.extend(v)
                    else:
                        flat.append(v)
                items = flat
        return items

    all_rows = []
    while True:
        _t0 = time.time()

        r = _get_with_backoff(url, params=params, timeout=_timeout())

        _sec = round(time.time() - _t0, 3)
        _raise_on_404(r, f"/libraries/{library_id}/entries")
        data = r.json()
        rows = _listify(data) or []

        if progress:
            try:
                progress({"rows": len(rows), "sec": _sec})
            except Exception:
                pass

        if rows and isinstance(rows[0], dict) and not rows[0].get("fields"):
            rows2 = []
            for e in rows:
                eid = e.get("id") or e.get("entry_id")
                if not eid:
                    rows2.append(e); continue
                durl = f"{base}/libraries/{library_id}/entries/{eid}"
                d = _get_with_backoff(durl, params=_token_params(), timeout=_timeout())
                _raise_on_404(d, f"/libraries/{library_id}/entries/{eid}")
                rows2.append(d.json())
            rows = rows2

        all_rows.extend(rows)

        next_url = None
        if isinstance(data, dict):
            next_url = data.get("next") or data.get("next_url")
            if not next_url:
                links = data.get("links") or {}
                if isinstance(links, dict):
                    next_url = links.get("next")
        if next_url:
            next_url = _up.urljoin(base + "/", next_url)
            parsed = _up.urlparse(next_url)
            q = dict(_up.parse_qsl(parsed.query))
            if "token" not in q:
                q.update(_token_params())
            url = _up.urlunparse(parsed._replace(query=_up.urlencode(q)))
            params = None
            continue

        token = None
        if isinstance(data, dict):
            token = data.get("nextPageToken") or data.get("next_page_token") or data.get("cursor") or data.get("continuation") or data.get("continuationToken")
        if token:
            url = f"{base}/libraries/{library_id}/entries"
            params = _token_params().copy()
            params["limit"] = int(limit)
            params["pageToken"] = token
            continue

        if isinstance(data, dict):
            total = data.get("total"); offset = data.get("offset")
            page = data.get("page"); pages = data.get("pages")
            if total is not None and offset is not None:
                n_off = int(offset) + int(limit)
                if n_off >= int(total): break
                url = f"{base}/libraries/{library_id}/entries"
                params = _token_params().copy(); params["limit"] = int(limit); params["offset"] = n_off
                continue
            if page is not None and pages is not None:
                p = int(page); P = int(pages)
                if p + 1 >= P: break
                url = f"{base}/libraries/{library_id}/entries"
                params = _token_params().copy(); params["limit"] = int(limit); params["page"] = p + 1
                continue

        break

    return all_rows

def probe_capabilities(library_id: str):
    base = _base_url().rstrip("/")
    url = f"{base}/libraries/{library_id}/entries"
    caps = {
        "accepts_sort": False,
        "sort_key": None,
        "accepts_updatedAfter": False,
        "accepts_createdAfter": False,
        "accepts_pageToken": False
    }

    r = _get_with_backoff(url, params={**_token_params(), "limit": 1}, timeout=_timeout())
    try:
        _ = r.json()
    except Exception:
        pass

    for key in ("modifiedTime", "updated_at", "updated", "tempo", "time"):
        rr = _get_with_backoff(url, params={**_token_params(), "limit": 1, "sort": key}, timeout=_timeout())
        if rr.status_code < 400:
            caps["accepts_sort"] = True
            caps["sort_key"] = key
            break

    for p in ("updatedAfter", "modifiedAfter"):
        rr = _get_with_backoff(url, params={**_token_params(), p: "1970-01-01T00:00:00Z", "limit": 1}, timeout=_timeout())
        if rr.status_code < 400:
            caps["accepts_updatedAfter"] = True
            break
    for p in ("createdAfter", "since"):
        rr = _get_with_backoff(url, params={**_token_params(), p: "1970-01-01T00:00:00Z", "limit": 1}, timeout=_timeout())
        if rr.status_code < 400:
            caps["accepts_createdAfter"] = True
            break

    rr = _get_with_backoff(url, params={**_token_params(), "limit": 1, "pageToken": "dummy"}, timeout=_timeout())
    if rr.status_code in (200, 400, 404):
        caps["accepts_pageToken"] = True

    return caps

def fetch_incremental(library_id: str, *, modified_after_iso: Optional[str] = None, since: Optional[str] = None, limit: int = 200, progress=None):
    # Backward-compatible alias: older callers pass `since=...`
    if modified_after_iso is None and since is not None:
        modified_after_iso = since

    caps = probe_capabilities(library_id)
    base = _base_url().rstrip("/")
    url = f"{base}/libraries/{library_id}/entries"

    params = _token_params().copy()
    params["limit"] = int(limit)

    if modified_after_iso and caps.get("accepts_updatedAfter"):
        params["updatedAfter"] = modified_after_iso
    elif modified_after_iso and caps.get("accepts_createdAfter"):
        params["createdAfter"] = modified_after_iso

    if caps.get("accepts_sort") and caps.get("sort_key"):
        params["sort"] = caps["sort_key"]

    rows = []
    while True:
        _t0 = time.time()

        r = _get_with_backoff(url, params=params, timeout=_timeout())
        _ensure_ok(r, f"list entries for {library_id}")

        _sec = round(time.time() - _t0, 3)
        _raise_on_404(r, f"/libraries/{library_id}/entries")
        data = r.json()
        chunk = (data.get("entries") if isinstance(data, dict) else data) or data
        if isinstance(chunk, dict):
            vals = list(chunk.values())
            if len(vals) == 1 and isinstance(vals[0], list):
                chunk = vals[0]
            else:
                flat = []
                for v in vals:
                    if isinstance(v, list):
                        flat.extend(v)
                    else:
                        flat.append(v)
                chunk = flat
        chunk = chunk or []
        if progress:
            try:
                ev = {"rows": len(chunk), "sec": _sec}
                if modified_after_iso:
                    ev["updatedAfter"] = modified_after_iso
                progress(ev)
            except Exception:
                pass
        rows.extend(chunk)

        token = None
        if isinstance(data, dict):
            token = data.get("nextPageToken") or data.get("next_page_token") or data.get("cursor") or data.get("continuation") or data.get("continuationToken")
        if token:
            params = _token_params().copy()
            params["limit"] = int(limit)
            if modified_after_iso:
                if caps.get("accepts_updatedAfter"):
                    params["updatedAfter"] = modified_after_iso
                elif caps.get("accepts_createdAfter"):
                    params["createdAfter"] = modified_after_iso
            if caps.get("accepts_sort") and caps.get("sort_key"):
                params["sort"] = caps["sort_key"]
            params["pageToken"] = token
            continue

        if isinstance(data, dict) and "total" in data and "offset" in data:
            total = int(data.get("total") or 0)
            offset = int(data.get("offset") or 0) + int(limit)
            if offset >= total:
                break
            params = _token_params().copy()
            params["limit"] = int(limit)
            if modified_after_iso:
                if caps.get("accepts_updatedAfter"):
                    params["updatedAfter"] = modified_after_iso
                elif caps.get("accepts_createdAfter"):
                    params["createdAfter"] = modified_after_iso
            if caps.get("accepts_sort") and caps.get("sort_key"):
                params["sort"] = caps["sort_key"]
            params["offset"] = offset
            continue

        break

    need_detail = rows and isinstance(rows[0], dict) and not rows[0].get("fields")
    if need_detail:
        total = len(rows)
        out: List[Dict[str, Any]] = []
        failed_ids: List[str] = []

        if progress:
            progress({"phase": "details_start", "total": total})

        for i, e in enumerate(rows, start=1):
            eid = e.get("id") or e.get("entry_id")
            if not eid:
                # keep raw if no id
                out.append(e)
                continue

            # progress tick (throttled by caller if desired)
            if progress and (i == 1 or i == total or i % 25 == 0):
                progress({"phase": "details", "done": i - 1, "total": total, "failed": len(failed_ids)})

            durl = f"{base}/libraries/{library_id}/entries/{eid}"
            try:
                resp = _get_with_backoff(durl, params=_token_params(), timeout=_timeout())
                _raise_on_404(resp, f"/libraries/{library_id}/entries/{eid}")
                _ensure_ok(resp, f"detail {eid}")
                out.append(resp.json())
            except Exception as ex:
                failed_ids.append(str(eid))
                if progress:
                    progress({
                        "phase": "detail_failed",
                        "entry_id": str(eid),
                        "done": i,
                        "total": total,
                        "failed": len(failed_ids),
                        "error": str(ex)[:200],
                    })
                # Skip this entry and continue import
                continue

        rows = out
        if progress:
            progress({"phase": "details", "done": total, "total": total, "failed": len(failed_ids)})
            if failed_ids:
                progress({"phase": "details_summary", "total": total, "failed": len(failed_ids)}) 

    return rows


def fetch_entry_detail(library_id: str, entry_id: str) -> Dict[str, Any]:
    """Fetch a single entry detail for enrichment.

    Tries a few common Memento API paths for compatibility across versions.
    Returns the parsed JSON dict (or raises for unexpected errors).
    """
    base = _base_url().rstrip("/")
    params = {}
    params.update(_token_params())
    # Candidate endpoints (try most likely first)
    candidates = [
        f"{base}/libraries/{library_id}/entries/{entry_id}",
        f"{base}/libraries/{library_id}/entry/{entry_id}",
        f"{base}/entries/{entry_id}",
    ]
    last = None
    for url in candidates:
        r = _get_with_backoff(url, params=params, timeout=_timeout())
        last = r
        # success
        if r.status_code < 400:
            try:
                return r.json()
            except Exception:
                return {"raw": r.text}
        # try next only on 404
        if r.status_code != 404:
            _raise_on_404(r, url)
            # for non-404 errors, raise a useful exception
            raise RuntimeError(f"fetch_entry_detail failed: {r.status_code} {r.text[:200]}")
    # if all candidates 404
    if last is not None:
        _raise_on_404(last, candidates[-1])
    raise RuntimeError("fetch_entry_detail failed: no response")

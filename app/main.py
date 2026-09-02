import hashlib, json, os, re, time, unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from threading import Lock, Thread

import libsql
import firebase_admin
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from firebase_admin import auth, credentials

from .parser import parse_pdf_bytes, repair_bangla, clean_field

APP_NAME = 'SV Tech Backend V21.3'
MAX_PDF_MB = int(os.getenv('MAX_PDF_MB', '100'))

# Preview -> upload usually happens immediately with the same PDF.  Cache parsed
# rows briefly so upload does not parse every PDF a second time.  The cache is
# best-effort only: a cold restart or another worker simply falls back to parsing.
PREVIEW_CACHE_TTL = int(os.getenv('PREVIEW_CACHE_TTL_SECONDS', '900'))
PREVIEW_CACHE_MAX = int(os.getenv('PREVIEW_CACHE_MAX_ITEMS', '6'))
_PREVIEW_CACHE = {}

# Search routing cache: (district, upazila) -> configured Turso database ids.
# It is built in the background and never blocks application startup. If the
# cache is missing/stale, public_search falls back to the full multi-DB scan,
# preserving result completeness.
_AREA_ROUTE_CACHE = {}
_AREA_ROUTE_LOCK = Lock()
_AREA_ROUTE_READY = False

# Name-only substring search index. A small FTS5 trigram index stores only the
# name column, so searches such as "সুমন" can find "মোহাম্মদ সুমন" without
# scanning every voter row in the area. The index is warmed in the background
# and maintained by SQLite triggers; the legacy LIKE path remains a correctness
# fallback if a database does not support FTS5/trigram.
_NAME_FTS_READY = set()
_NAME_FTS_FAILED = set()
_NAME_FTS_LOCK = Lock()
NAME_FTS_VERSION = 'name-trigram-v1'

# V21.2 token index for Bangladeshi personal names. unicode61 indexes complete
# name-parts (first/middle/last/surname) instead of scanning the voter table.
_PERSON_FTS_READY = set()
_PERSON_FTS_FAILED = set()
_PERSON_FTS_LOCK = Lock()
PERSON_FTS_VERSION = 'person-token-v1'

# Conservative spelling aliases for common Bengali name-parts.
_NAME_ALIAS_GROUPS = (
    ('মিয়া', 'মিয়া', 'মিঞা'),
    ('আলী', 'আলি'),
    ('হোসেন', 'হোসাইন'),
    ('উদ্দিন', 'উদ্দীন'),
)
_NAME_ALIAS_MAP = {}
for _group in _NAME_ALIAS_GROUPS:
    _clean_group = tuple(dict.fromkeys(unicodedata.normalize('NFC', x) for x in _group))
    for _token in _clean_group:
        _NAME_ALIAS_MAP[_token] = _clean_group

# Search-only trigram index for father name, mother name, and birth date.
# It is kept separate from the voter table and maintained by triggers so
# single-field parent/DOB searches do not scan every row in an area.
_AUX_FTS_READY = set()
_AUX_FTS_FAILED = set()
_AUX_FTS_LOCK = Lock()
AUX_FTS_VERSION = 'parent-dob-trigram-v1'

# V21 location search index. Village searches existing address/voter_area text.
_GEO_FTS_READY = set()
_GEO_FTS_FAILED = set()
_GEO_FTS_LOCK = Lock()
GEO_FTS_VERSION = 'address-area-ward-trigram-v1'
SEARCH_WORKERS = max(1, int(os.getenv('SEARCH_WORKERS', '8')))

_BN_DIGITS = '০১২৩৪৫৬৭৮৯'
_ASCII_DIGITS = '0123456789'
_BN_TO_ASCII = str.maketrans(_BN_DIGITS, _ASCII_DIGITS)
_ASCII_TO_BN = str.maketrans(_ASCII_DIGITS, _BN_DIGITS)

# Dashboard/storage metrics cache. Remote Turso databases are queried in parallel,
# then the aggregate is kept briefly in memory. Stale cached data is returned
# immediately while a daemon thread refreshes it in the background.
DASHBOARD_CACHE_TTL = int(os.getenv('DASHBOARD_CACHE_TTL_SECONDS', '120'))
_METRICS_CACHE = {'stats': None, 'usage': None}
_METRICS_CACHE_LOCK = Lock()
_METRICS_REFRESHING = {'stats': False, 'usage': False}

def _area_key(district: str, upazila: str):
    return (str(district or '').strip(), str(upazila or '').strip())

def _route_get(district: str, upazila: str):
    key = _area_key(district, upazila)
    with _AREA_ROUTE_LOCK:
        return tuple(_AREA_ROUTE_CACHE.get(key, ()))

def _route_add(district: str, upazila: str, database_id: str):
    key = _area_key(district, upazila)
    if not key[0] or not key[1] or not database_id:
        return
    with _AREA_ROUTE_LOCK:
        current = set(_AREA_ROUTE_CACHE.get(key, ()))
        current.add(str(database_id))
        _AREA_ROUTE_CACHE[key] = tuple(sorted(current))

def _rebuild_area_route_cache():
    global _AREA_ROUTE_READY
    local = {}
    items = turso_catalog(False)

    def scan(item):
        conn = None
        try:
            conn = connect_item(item, ensure=False)
            rows = conn.execute(
                "SELECT DISTINCT district_name,upazila_name FROM records "
                "WHERE district_name<>'' AND upazila_name<>''"
            ).fetchall()
            return item['id'], rows
        except Exception:
            return item['id'], []
        finally:
            if conn is not None:
                try: conn.close()
                except Exception: pass

    if items:
        with ThreadPoolExecutor(max_workers=max(1, min(len(items), 5))) as pool:
            for dbid, rows in pool.map(scan, items):
                for district, upazila in rows:
                    key = _area_key(district, upazila)
                    if not key[0] or not key[1]:
                        continue
                    local.setdefault(key, set()).add(str(dbid))
    with _AREA_ROUTE_LOCK:
        _AREA_ROUTE_CACHE.clear()
        _AREA_ROUTE_CACHE.update({k: tuple(sorted(v)) for k, v in local.items()})
        _AREA_ROUTE_READY = True

def _start_route_cache_builder():
    try:
        Thread(target=_rebuild_area_route_cache, name='area-route-cache', daemon=True).start()
    except Exception:
        pass

def _name_fts_is_ready(database_id: str) -> bool:
    with _NAME_FTS_LOCK:
        return str(database_id) in _NAME_FTS_READY

def _person_fts_is_ready(database_id: str) -> bool:
    with _PERSON_FTS_LOCK:
        return str(database_id) in _PERSON_FTS_READY

def _aux_fts_is_ready(database_id: str) -> bool:
    with _AUX_FTS_LOCK:
        return str(database_id) in _AUX_FTS_READY

def _geo_fts_is_ready(database_id: str) -> bool:
    with _GEO_FTS_LOCK:
        return str(database_id) in _GEO_FTS_READY


def _probe_fts_ready_on_conn(conn, database_id: str):
    """Load persisted FTS readiness markers using one cheap remote query.

    After a deploy the in-memory READY sets start empty even though the Turso
    FTS tables are already built. Without this probe, the first public searches
    fall back to expensive instr() scans until the background warmer reaches
    that database.
    """
    dbid = str(database_id or '')
    if not dbid:
        return
    need_name = not _name_fts_is_ready(dbid)
    need_person = not _person_fts_is_ready(dbid)
    need_aux = not _aux_fts_is_ready(dbid)
    need_geo = not _geo_fts_is_ready(dbid)
    if not (need_name or need_person or need_aux or need_geo):
        return
    try:
        rows = conn.execute(
            "SELECT key FROM search_index_meta WHERE key IN (?,?,?,?)",
            (NAME_FTS_VERSION, PERSON_FTS_VERSION, AUX_FTS_VERSION, GEO_FTS_VERSION),
        ).fetchall()
    except Exception:
        return
    keys = {str(r[0]) for r in rows}
    if NAME_FTS_VERSION in keys:
        with _NAME_FTS_LOCK:
            _NAME_FTS_READY.add(dbid); _NAME_FTS_FAILED.discard(dbid)
    if PERSON_FTS_VERSION in keys:
        with _PERSON_FTS_LOCK:
            _PERSON_FTS_READY.add(dbid); _PERSON_FTS_FAILED.discard(dbid)
    if AUX_FTS_VERSION in keys:
        with _AUX_FTS_LOCK:
            _AUX_FTS_READY.add(dbid); _AUX_FTS_FAILED.discard(dbid)
    if GEO_FTS_VERSION in keys:
        with _GEO_FTS_LOCK:
            _GEO_FTS_READY.add(dbid); _GEO_FTS_FAILED.discard(dbid)


def _resolve_area_items(all_items, district: str, upazila: str):
    """Return every configured DB that actually contains the selected area.

    The area probe uses the existing (district_name, upazila_name) B-tree index,
    so it is much cheaper than running a partial-name scan against every DB.
    Results are cached for later searches. This keeps completeness even when the
    global route-cache warmer has not finished yet.
    """
    item_by_id = {str(x['id']): x for x in all_items}
    routed_ids = _route_get(district, upazila)
    if _AREA_ROUTE_READY and routed_ids:
        return [item_by_id[x] for x in routed_ids if x in item_by_id], True

    def has_area(item):
        conn = None
        try:
            conn = connect_item(item, ensure=False)
            row = conn.execute(
                "SELECT 1 FROM records WHERE district_name=? AND upazila_name=? LIMIT 1",
                (district, upazila),
            ).fetchone()
            return item if row else None
        except Exception:
            return None
        finally:
            if conn is not None:
                try: conn.close()
                except Exception: pass

    found = []
    if all_items:
        workers = max(1, min(len(all_items), SEARCH_WORKERS))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for item in pool.map(has_area, all_items):
                if item is not None:
                    found.append(item)
                    _route_add(district, upazila, item['id'])
    return found, False

def _ascii_digits(value: str) -> str:
    return str(value or '').translate(_BN_TO_ASCII)

def _parse_dob_query(value: str):
    """Return (year, canonical YYYY-MM-DD or ''). Accept common DOB formats."""
    raw=_ascii_digits(value).strip()
    if not raw:
        return '', ''
    if re.fullmatch(r'(?:19|20)\d{2}', raw):
        return raw, ''
    parts=re.findall(r'\d+', raw)
    y=m=d=None
    if len(parts)==3:
        if len(parts[0])==4:
            y,m,d=parts
        elif len(parts[2])==4:
            d,m,y=parts
    elif len(parts)==1 and len(parts[0])==8:
        digits=parts[0]
        if re.fullmatch(r'(?:19|20)\d{6}', digits):
            y,m,d=digits[:4],digits[4:6],digits[6:8]
        elif re.fullmatch(r'\d{4}(?:19|20)\d{2}', digits):
            d,m,y=digits[:2],digits[2:4],digits[4:8]
    if y and m and d:
        try:
            dt=datetime(int(y),int(m),int(d))
            return f'{dt.year:04d}', f'{dt.year:04d}-{dt.month:02d}-{dt.day:02d}'
        except Exception:
            pass
    # Even if the full date is malformed/unknown, a visible 4-digit year can
    # still narrow the FTS candidate set before the compatibility fallback.
    year_match=re.search(r'(?<!\d)(?:19|20)\d{2}(?!\d)', raw)
    return (year_match.group(0) if year_match else ''), ''

def _dob_matches(stored: str, query: str) -> bool:
    qyear,qcanon=_parse_dob_query(query)
    syear,scanon=_parse_dob_query(stored)
    if qcanon:
        if scanon:
            return scanon == qcanon
        return _ascii_digits(query).strip() in _ascii_digits(stored).strip()
    if qyear:
        return syear == qyear or qyear in _ascii_digits(stored)
    return _ascii_digits(query).strip() in _ascii_digits(stored).strip()

def _fts_year_query(year: str) -> str:
    terms=[]
    for term in (year, str(year).translate(_ASCII_TO_BN)):
        term=str(term or '').strip()
        if term and term not in terms:
            terms.append(term)
    return ' OR '.join('"'+x.replace('"','""')+'"' for x in terms)

def _ensure_name_search_index(item):
    """Create/backfill the search-only trigram index once per Turso DB.

    This never changes voter data. Triggers keep the name index synchronized
    with future INSERT/UPDATE/DELETE operations without changing upload code.
    """
    dbid = str(item.get('id', ''))
    with _NAME_FTS_LOCK:
        if dbid in _NAME_FTS_READY:
            return True
        if dbid in _NAME_FTS_FAILED:
            return False
    conn = None
    try:
        conn = connect_item(item, ensure=False)
        conn.execute("CREATE TABLE IF NOT EXISTS search_index_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '')")
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS records_name_fts USING fts5(name, tokenize='trigram')")
        conn.execute('''CREATE TRIGGER IF NOT EXISTS records_name_fts_ai AFTER INSERT ON records BEGIN
            INSERT INTO records_name_fts(rowid,name) VALUES (new.rowid,COALESCE(new.name,''));
        END''')
        conn.execute('''CREATE TRIGGER IF NOT EXISTS records_name_fts_ad AFTER DELETE ON records BEGIN
            DELETE FROM records_name_fts WHERE rowid=old.rowid;
        END''')
        conn.execute('''CREATE TRIGGER IF NOT EXISTS records_name_fts_au AFTER UPDATE ON records BEGIN
            DELETE FROM records_name_fts WHERE rowid=old.rowid;
            INSERT INTO records_name_fts(rowid,name) VALUES (new.rowid,COALESCE(new.name,''));
        END''')
        marker = conn.execute("SELECT value FROM search_index_meta WHERE key=?", (NAME_FTS_VERSION,)).fetchone()
        if not marker:
            # One-time backfill for records that existed before this version.
            conn.execute('DELETE FROM records_name_fts')
            conn.execute("INSERT INTO records_name_fts(rowid,name) SELECT rowid,COALESCE(name,'') FROM records")
            conn.execute("INSERT OR REPLACE INTO search_index_meta(key,value) VALUES (?,?)",
                         (NAME_FTS_VERSION, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        with _NAME_FTS_LOCK:
            _NAME_FTS_READY.add(dbid)
            _NAME_FTS_FAILED.discard(dbid)
        return True
    except Exception:
        # Do not make public search depend on optional FTS support. The caller
        # will use the complete legacy LIKE scan for this database instead.
        with _NAME_FTS_LOCK:
            _NAME_FTS_FAILED.add(dbid)
        return False
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass

def _name_tokens(value: str):
    """Canonical Unicode word tokens used for person-name search/ranking."""
    text = unicodedata.normalize('NFC', str(value or ''))
    text = text.replace('\u200c', '').replace('\u200d', '').replace('\ufeff', '')
    text = ''.join(ch if (ch.isalnum() or unicodedata.category(ch).startswith('M')) else ' ' for ch in text)
    return [x for x in text.split() if x]


def _token_aliases(token: str):
    token = unicodedata.normalize('NFC', str(token or '').strip())
    return _NAME_ALIAS_MAP.get(token, (token,)) if token else ()


def _fts_quote(value: str):
    return '"' + str(value).replace('"', '""') + '"'


def _person_field_match(column: str, value: str):
    """Build FTS5 expression requiring every supplied name-part in one field."""
    tokens = _name_tokens(value)
    if not tokens:
        return ''
    groups = []
    for token in tokens:
        variants = tuple(dict.fromkeys(_token_aliases(token)))
        alts = ' OR '.join(f'{column}:{_fts_quote(v)}' for v in variants)
        groups.append('(' + alts + ')')
    return ' AND '.join(groups)


def _person_match_query(name: str = '', father: str = '', mother: str = ''):
    parts = []
    for column, value in (('name', name), ('father_name', father), ('mother_name', mother)):
        expr = _person_field_match(column, value)
        if expr:
            parts.append('(' + expr + ')')
    return ' AND '.join(parts)


def _token_value_matches(value: str, query: str):
    if not query:
        return True
    value_tokens = set(_name_tokens(value))
    for q in _name_tokens(query):
        if not any(alias in value_tokens for alias in _token_aliases(q)):
            return False
    return True


def _ensure_person_search_index(item):
    """Create/backfill V21.2 unicode word-token index for name/father/mother."""
    dbid = str(item.get('id', ''))
    with _PERSON_FTS_LOCK:
        if dbid in _PERSON_FTS_READY:
            return True
        if dbid in _PERSON_FTS_FAILED:
            return False
    conn = None
    try:
        conn = connect_item(item, ensure=False)
        conn.execute("CREATE TABLE IF NOT EXISTS search_index_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '')")
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS records_person_fts USING fts5(name,father_name,mother_name, tokenize='unicode61')")
        conn.execute("""CREATE TRIGGER IF NOT EXISTS records_person_fts_ai AFTER INSERT ON records BEGIN
            INSERT INTO records_person_fts(rowid,name,father_name,mother_name)
            VALUES (new.rowid,COALESCE(new.name,''),COALESCE(new.father_name,''),COALESCE(new.mother_name,''));
        END""")
        conn.execute("""CREATE TRIGGER IF NOT EXISTS records_person_fts_ad AFTER DELETE ON records BEGIN
            DELETE FROM records_person_fts WHERE rowid=old.rowid;
        END""")
        conn.execute("""CREATE TRIGGER IF NOT EXISTS records_person_fts_au AFTER UPDATE ON records BEGIN
            DELETE FROM records_person_fts WHERE rowid=old.rowid;
            INSERT INTO records_person_fts(rowid,name,father_name,mother_name)
            VALUES (new.rowid,COALESCE(new.name,''),COALESCE(new.father_name,''),COALESCE(new.mother_name,''));
        END""")
        marker = conn.execute("SELECT value FROM search_index_meta WHERE key=?", (PERSON_FTS_VERSION,)).fetchone()
        if not marker:
            conn.execute('DELETE FROM records_person_fts')
            conn.execute("INSERT INTO records_person_fts(rowid,name,father_name,mother_name) SELECT rowid,COALESCE(name,''),COALESCE(father_name,''),COALESCE(mother_name,'') FROM records")
            conn.execute("INSERT OR REPLACE INTO search_index_meta(key,value) VALUES (?,?)",
                         (PERSON_FTS_VERSION, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        with _PERSON_FTS_LOCK:
            _PERSON_FTS_READY.add(dbid)
            _PERSON_FTS_FAILED.discard(dbid)
        return True
    except Exception:
        with _PERSON_FTS_LOCK:
            _PERSON_FTS_FAILED.add(dbid)
        return False
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass


def _ensure_aux_search_index(item):
    """Create/backfill the parent-name and DOB trigram index once per DB."""
    dbid=str(item.get('id',''))
    with _AUX_FTS_LOCK:
        if dbid in _AUX_FTS_READY:
            return True
        if dbid in _AUX_FTS_FAILED:
            return False
    conn=None
    try:
        conn=connect_item(item, ensure=False)
        conn.execute("CREATE TABLE IF NOT EXISTS search_index_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '')")
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS records_aux_fts USING fts5(father_name,mother_name,birth_date, tokenize='trigram')")
        conn.execute('''CREATE TRIGGER IF NOT EXISTS records_aux_fts_ai AFTER INSERT ON records BEGIN
            INSERT INTO records_aux_fts(rowid,father_name,mother_name,birth_date)
            VALUES (new.rowid,COALESCE(new.father_name,''),COALESCE(new.mother_name,''),COALESCE(new.birth_date,''));
        END''')
        conn.execute('''CREATE TRIGGER IF NOT EXISTS records_aux_fts_ad AFTER DELETE ON records BEGIN
            DELETE FROM records_aux_fts WHERE rowid=old.rowid;
        END''')
        conn.execute('''CREATE TRIGGER IF NOT EXISTS records_aux_fts_au AFTER UPDATE ON records BEGIN
            DELETE FROM records_aux_fts WHERE rowid=old.rowid;
            INSERT INTO records_aux_fts(rowid,father_name,mother_name,birth_date)
            VALUES (new.rowid,COALESCE(new.father_name,''),COALESCE(new.mother_name,''),COALESCE(new.birth_date,''));
        END''')
        marker=conn.execute("SELECT value FROM search_index_meta WHERE key=?", (AUX_FTS_VERSION,)).fetchone()
        if not marker:
            conn.execute('DELETE FROM records_aux_fts')
            conn.execute("INSERT INTO records_aux_fts(rowid,father_name,mother_name,birth_date) SELECT rowid,COALESCE(father_name,''),COALESCE(mother_name,''),COALESCE(birth_date,'') FROM records")
            conn.execute("INSERT OR REPLACE INTO search_index_meta(key,value) VALUES (?,?)",
                         (AUX_FTS_VERSION, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        with _AUX_FTS_LOCK:
            _AUX_FTS_READY.add(dbid)
            _AUX_FTS_FAILED.discard(dbid)
        return True
    except Exception:
        with _AUX_FTS_LOCK:
            _AUX_FTS_FAILED.add(dbid)
        return False
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass

def _ensure_geo_search_index(item):
    """Create/backfill the V21 village/address/voter-area/ward trigram index."""
    dbid=str(item.get('id',''))
    with _GEO_FTS_LOCK:
        if dbid in _GEO_FTS_READY:
            return True
        if dbid in _GEO_FTS_FAILED:
            return False
    conn=None
    try:
        conn=connect_item(item, ensure=False)
        conn.execute("CREATE TABLE IF NOT EXISTS search_index_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '')")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_records_ward ON records(ward_no)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_records_area_ward ON records(district_name, upazila_name, ward_no)")
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS records_geo_fts USING fts5(address,voter_area,ward_no, tokenize='trigram')")
        conn.execute("CREATE TRIGGER IF NOT EXISTS records_geo_fts_ai AFTER INSERT ON records BEGIN INSERT INTO records_geo_fts(rowid,address,voter_area,ward_no) VALUES (new.rowid,COALESCE(new.address,''),COALESCE(new.voter_area,''),COALESCE(new.ward_no,'')); END")
        conn.execute("CREATE TRIGGER IF NOT EXISTS records_geo_fts_ad AFTER DELETE ON records BEGIN DELETE FROM records_geo_fts WHERE rowid=old.rowid; END")
        conn.execute("CREATE TRIGGER IF NOT EXISTS records_geo_fts_au AFTER UPDATE ON records BEGIN DELETE FROM records_geo_fts WHERE rowid=old.rowid; INSERT INTO records_geo_fts(rowid,address,voter_area,ward_no) VALUES (new.rowid,COALESCE(new.address,''),COALESCE(new.voter_area,''),COALESCE(new.ward_no,'')); END")
        marker=conn.execute("SELECT value FROM search_index_meta WHERE key=?", (GEO_FTS_VERSION,)).fetchone()
        if not marker:
            conn.execute('DELETE FROM records_geo_fts')
            conn.execute("INSERT INTO records_geo_fts(rowid,address,voter_area,ward_no) SELECT rowid,COALESCE(address,''),COALESCE(voter_area,''),COALESCE(ward_no,'') FROM records")
            conn.execute("INSERT OR REPLACE INTO search_index_meta(key,value) VALUES (?,?)", (GEO_FTS_VERSION, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        with _GEO_FTS_LOCK:
            _GEO_FTS_READY.add(dbid)
            _GEO_FTS_FAILED.discard(dbid)
        return True
    except Exception:
        with _GEO_FTS_LOCK:
            _GEO_FTS_FAILED.add(dbid)
        return False
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass

def _rebuild_name_search_indexes():
    items = turso_catalog(False)
    if not items:
        return
    # Keep startup/network pressure modest; index construction is server-side.
    def build(item):
        # Existing indexes are normally already persisted from V21.1; mark them
        # ready first so searches stay fast while the new person-token index is
        # backfilled once in the background.
        _ensure_name_search_index(item)
        _ensure_aux_search_index(item)
        _ensure_geo_search_index(item)
        _ensure_person_search_index(item)
    with ThreadPoolExecutor(max_workers=max(1, min(len(items), 2))) as pool:
        list(pool.map(build, items))

def _start_name_search_index_builder():
    try:
        Thread(target=_rebuild_name_search_indexes, name='name-search-index', daemon=True).start()
    except Exception:
        pass

def _parse_cache_key(data: bytes, district: str, upazila: str) -> str:
    h = hashlib.sha256()
    h.update(data)
    h.update(b'\0')
    h.update(district.strip().encode('utf-8'))
    h.update(b'\0')
    h.update(upazila.strip().encode('utf-8'))
    return h.hexdigest()

def _cache_put(key: str, rows):
    now = time.time()
    # Drop stale entries first.
    for k, item in list(_PREVIEW_CACHE.items()):
        if now - float(item.get('ts', 0)) > PREVIEW_CACHE_TTL:
            _PREVIEW_CACHE.pop(k, None)
    if len(_PREVIEW_CACHE) >= PREVIEW_CACHE_MAX:
        oldest = min(_PREVIEW_CACHE, key=lambda k: _PREVIEW_CACHE[k].get('ts', 0))
        _PREVIEW_CACHE.pop(oldest, None)
    _PREVIEW_CACHE[key] = {'ts': now, 'rows': rows}

def _cache_get(key: str):
    item = _PREVIEW_CACHE.get(key)
    if not item:
        return None
    if time.time() - float(item.get('ts', 0)) > PREVIEW_CACHE_TTL:
        _PREVIEW_CACHE.pop(key, None)
        return None
    return item.get('rows')
ADMIN_EMAILS = {x.strip().lower() for x in os.getenv('ADMIN_EMAILS', '').split(',') if x.strip()}
SKIP_AUTH = os.getenv('SKIP_AUTH', '').lower() in {'1','true','yes'}

# Firebase is retained ONLY for the existing Admin login/token verification.
# All application records are stored in Turso; Firestore is not used.
def init_auth_only():
    if SKIP_AUTH or firebase_admin._apps:
        return
    raw = os.getenv('FIREBASE_SERVICE_ACCOUNT_JSON', '').strip()
    if raw:
        firebase_admin.initialize_app(credentials.Certificate(json.loads(raw)))
        return
    path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '').strip()
    if path:
        firebase_admin.initialize_app(credentials.Certificate(path))
        return
    raise RuntimeError('Admin login verification needs FIREBASE_SERVICE_ACCOUNT_JSON, or set SKIP_AUTH=true for local testing')

init_auth_only()

app = FastAPI(title=APP_NAME, version='21.4.0')
origins = [x.strip() for x in os.getenv('ALLOWED_ORIGINS','*').split(',') if x.strip()]
app.add_middleware(CORSMiddleware, allow_origins=origins or ['*'], allow_credentials=False,
                   allow_methods=['GET','POST','DELETE','OPTIONS'], allow_headers=['*'])

@app.api_route('/', methods=['GET', 'HEAD'], include_in_schema=False)
def health_root():
    return {'status': 'ok', 'service': APP_NAME, 'version': '21.4.0'}

@app.on_event('startup')
def _warm_background_caches():
    # Do not block Railway startup. Warm search routing plus dashboard/storage
    # metrics in daemon threads so the first admin page load is usually instant.
    _start_route_cache_builder()
    _start_name_search_index_builder()
    _start_metrics_refresh('stats')
    _start_metrics_refresh('usage')


async def current_user(authorization: str | None = Header(default=None)):
    if SKIP_AUTH:
        return {'email':'local@test','uid':'local'}
    if not authorization or not authorization.lower().startswith('bearer '):
        raise HTTPException(401, 'Admin login token পাওয়া যায়নি')
    try:
        decoded = auth.verify_id_token(authorization.split(' ',1)[1].strip())
    except Exception as e:
        raise HTTPException(401, 'Admin login token invalid') from e
    email = str(decoded.get('email','')).lower()
    if ADMIN_EMAILS and email not in ADMIN_EMAILS:
        raise HTTPException(403, 'এই account-এর API permission নেই')
    return decoded


def clean_id(value):
    out = ''.join(ch.lower() if ch.isalnum() else '-' for ch in str(value).strip())
    return '-'.join(x for x in out.split('-') if x)[:80] or 'database'


def turso_catalog(include_disabled=True):
    items = []
    # Dynamic multi-database discovery. Add DB6, DB7, DB25, DB100... without code changes:
    # TURSO_DATABASE_<N>_URL + TURSO_DATABASE_<N>_AUTH_TOKEN.
    slots = set()
    for key in os.environ:
        m = re.fullmatch(r'TURSO_DATABASE_(\d+)_(?:URL|AUTH_TOKEN|ENABLED|NAME|ACCOUNT|ID|LIMIT_GB)', key)
        if m:
            slots.add(int(m.group(1)))
    for i in sorted(x for x in slots if x > 0):
        url = os.getenv(f'TURSO_DATABASE_{i}_URL','').strip()
        token = os.getenv(f'TURSO_DATABASE_{i}_AUTH_TOKEN','').strip()
        if not url or not token:
            continue
        enabled = os.getenv(f'TURSO_DATABASE_{i}_ENABLED','true').strip().lower() not in {'0','false','no','off'}
        if not include_disabled and not enabled:
            continue
        name = os.getenv(f'TURSO_DATABASE_{i}_NAME', f'Turso DB {i}').strip() or f'Turso DB {i}'
        account = os.getenv(f'TURSO_DATABASE_{i}_ACCOUNT', f'Turso Account {i}').strip() or f'Turso Account {i}'
        dbid = os.getenv(f'TURSO_DATABASE_{i}_ID', f'db{i}').strip() or f'db{i}'
        items.append({'id': clean_id(dbid), 'name': name, 'account': account, 'url': url, 'token': token, 'enabled': enabled, 'slot': i})
    # Single-database compatibility.
    if not items:
        url = os.getenv('TURSO_DATABASE_URL','').strip()
        token = os.getenv('TURSO_AUTH_TOKEN','').strip()
        if url and token:
            items.append({'id':'primary','name':os.getenv('TURSO_DATABASE_NAME','Primary Turso') or 'Primary Turso',
                          'url':url,'token':token,'enabled':True,'slot':1})
    return items


def get_target(database_id=''):
    all_items = turso_catalog(include_disabled=True)
    if not all_items:
        raise HTTPException(503, 'Turso database configure করা হয়নি')
    if not database_id:
        item = next((x for x in all_items if x['enabled']), None)
    else:
        item = next((x for x in all_items if x['id']==database_id), None)
    if not item:
        raise HTTPException(404, 'Turso database configuration পাওয়া যায়নি')
    if not item['enabled']:
        raise HTTPException(400, 'এই Turso database disabled আছে')
    return item


def connect_item(item, ensure=True):
    conn = libsql.connect(database=item['url'], auth_token=item['token'])
    # Read-only search requests must not repeat remote CREATE TABLE/INDEX checks.
    # Schema is ensured by upload/admin paths; skipping it here removes several
    # network round-trips from every public search.
    if ensure:
        ensure_schema(conn)
    return conn


def ensure_schema(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS records (
        record_key TEXT PRIMARY KEY,
        voter_no TEXT,
        serial_no TEXT,
        name TEXT,
        father_name TEXT,
        mother_name TEXT,
        profession TEXT,
        birth_date TEXT,
        district_name TEXT NOT NULL DEFAULT '',
        upazila_name TEXT NOT NULL DEFAULT '',
        address TEXT,
        union_name TEXT,
        post_office TEXT,
        post_code TEXT,
        voter_area TEXT,
        voter_area_code TEXT,
        ward_no TEXT,
        source_file TEXT,
        created_at TEXT,
        data_json TEXT NOT NULL
    )''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_records_area ON records(district_name, upazila_name)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_records_voter ON records(voter_no)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_records_name ON records(name)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_records_father ON records(father_name)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_records_mother ON records(mother_name)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_records_dob ON records(birth_date)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_records_area_name ON records(district_name, upazila_name, name)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_records_area_father ON records(district_name, upazila_name, father_name)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_records_area_mother ON records(district_name, upazila_name, mother_name)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_records_area_dob ON records(district_name, upazila_name, birth_date)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_records_ward ON records(ward_no)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_records_area_ward ON records(district_name, upazila_name, ward_no)')
    conn.execute('''CREATE TABLE IF NOT EXISTS pdf_imports (
        id TEXT PRIMARY KEY, database_id TEXT, district_name TEXT, upazila_name TEXT,
        file_name TEXT, records_detected INTEGER, records_written INTEGER,
        created_at TEXT, uploaded_by TEXT, parser TEXT
    )''')
    conn.commit()


def safe_record_key(row):
    voter = str(row.get('voter_no','')).strip()
    if voter:
        return 'v:' + voter
    basis = str(row.get('record_key') or '|'.join(str(row.get(k,'')) for k in ('district_name','upazila_name','source_file','serial_no')))
    return 's:' + hashlib.sha1(basis.encode('utf-8')).hexdigest()


def row_params(row):
    r = dict(row)
    key = safe_record_key(r)
    r['record_key'] = key
    fields = ['voter_no','serial_no','name','father_name','mother_name','profession','birth_date','district_name','upazila_name',
              'address','union_name','post_office','post_code','voter_area','voter_area_code','ward_no','source_file','created_at']
    vals = [str(r.get(k,'') or '') for k in fields]
    return [key, *vals, json.dumps(r, ensure_ascii=False, separators=(',',':'))]

UPSERT_SQL = '''INSERT INTO records (
 record_key,voter_no,serial_no,name,father_name,mother_name,profession,birth_date,district_name,upazila_name,
 address,union_name,post_office,post_code,voter_area,voter_area_code,ward_no,source_file,created_at,data_json
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(record_key) DO UPDATE SET
 voter_no=excluded.voter_no,serial_no=excluded.serial_no,name=excluded.name,father_name=excluded.father_name,
 mother_name=excluded.mother_name,profession=excluded.profession,birth_date=excluded.birth_date,
 district_name=excluded.district_name,upazila_name=excluded.upazila_name,address=excluded.address,
 union_name=excluded.union_name,post_office=excluded.post_office,post_code=excluded.post_code,
 voter_area=excluded.voter_area,voter_area_code=excluded.voter_area_code,ward_no=excluded.ward_no,
 source_file=excluded.source_file,created_at=excluded.created_at,data_json=excluded.data_json'''


def _comparable_record(row):
    """Return stable record content for upload-result comparison.

    Parser/import metadata changes on every upload (notably created_at) and must not
    turn an otherwise identical voter record into an "updated" record.
    """
    r = dict(row or {})
    for key in ('created_at', 'record_key', 'source_file'):
        r.pop(key, None)
    return r


def write_rows(rows, item, import_meta=None):
    # The schema already exists for configured Turso databases. Avoid repeating
    # CREATE TABLE/INDEX checks on every upload; those remote DDL round-trips
    # were a major part of post-preview upload latency.
    conn = connect_item(item, ensure=False)
    try:
        # Classify incoming rows before the UPSERT so the Admin Panel can report
        # exact new / updated / unchanged counts instead of showing 0 for all.
        params_by_key = {}
        row_by_key = {}
        duplicate_keys = 0
        for row in rows:
            params = row_params(row)
            key = params[0]
            if key in row_by_key:
                duplicate_keys += 1
            row_by_key[key] = row
            params_by_key[key] = params

        keys = list(row_by_key)
        existing = {}
        # Keep well below SQLite's host-parameter limit and avoid one remote read
        # per record.
        for start in range(0, len(keys), 800):
            chunk = keys[start:start + 800]
            if not chunk:
                continue
            marks = ','.join('?' for _ in chunk)
            sql = f'SELECT record_key,data_json FROM records WHERE record_key IN ({marks})'
            for record_key, data_json in conn.execute(sql, chunk).fetchall():
                try:
                    existing[str(record_key)] = json.loads(data_json or '{}')
                except Exception:
                    existing[str(record_key)] = {}

        added = updated = unchanged = 0
        to_write = []
        for key in keys:
            incoming = row_by_key[key]
            previous = existing.get(key)
            if previous is None:
                added += 1
                to_write.append(params_by_key[key])
            elif _comparable_record(previous) == _comparable_record(incoming):
                unchanged += 1
            else:
                updated += 1
                to_write.append(params_by_key[key])

        if to_write:
            # libSQL is remote here. DB-API executemany may still issue many remote
            # statements, which is very expensive for an 800+ row PDF. Send true
            # multi-row INSERTs instead. 40 rows x 20 columns = 800 parameters,
            # safely below SQLite's conservative 999-variable limit.
            columns = ('record_key,voter_no,serial_no,name,father_name,mother_name,profession,birth_date,'
                       'district_name,upazila_name,address,union_name,post_office,post_code,voter_area,'
                       'voter_area_code,ward_no,source_file,created_at,data_json')
            update_sql = '''ON CONFLICT(record_key) DO UPDATE SET
 voter_no=excluded.voter_no,serial_no=excluded.serial_no,name=excluded.name,father_name=excluded.father_name,
 mother_name=excluded.mother_name,profession=excluded.profession,birth_date=excluded.birth_date,
 district_name=excluded.district_name,upazila_name=excluded.upazila_name,address=excluded.address,
 union_name=excluded.union_name,post_office=excluded.post_office,post_code=excluded.post_code,
 voter_area=excluded.voter_area,voter_area_code=excluded.voter_area_code,ward_no=excluded.ward_no,
 source_file=excluded.source_file,created_at=excluded.created_at,data_json=excluded.data_json'''
            for start in range(0, len(to_write), 40):
                batch = to_write[start:start + 40]
                values_sql = ','.join(['(' + ','.join(['?'] * 20) + ')'] * len(batch))
                flat = [value for params in batch for value in params]
                conn.execute(f'INSERT INTO records ({columns}) VALUES {values_sql} {update_sql}', flat)

        # Keep the import audit row in the SAME connection/transaction as the
        # record write. This removes a second Turso connection + schema check +
        # commit from every upload.
        if import_meta:
            iid='import_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
            conn.execute(
                'INSERT INTO pdf_imports(id,database_id,district_name,upazila_name,file_name,records_detected,records_written,created_at,uploaded_by,parser) VALUES (?,?,?,?,?,?,?,?,?,?)',
                (iid, import_meta.get('database_id',''), import_meta.get('district_name',''),
                 import_meta.get('upazila_name',''), import_meta.get('file_name',''),
                 int(import_meta.get('records_detected',0)), added + updated,
                 import_meta.get('created_at',''), import_meta.get('uploaded_by',''),
                 import_meta.get('parser',''))
            )

        if to_write or import_meta:
            conn.commit()

        return {
            'records_added': added,
            'records_updated': updated,
            'records_unchanged': unchanged,
            'records_skipped': 0,
            'duplicate_input_keys': duplicate_keys,
            'records_written': added + updated,
            'batch_commits': 1 if to_write else 0,
        }
    finally:
        conn.close()


def record_from_db(row):
    # SELECT returns the stored full JSON so new parser fields are preserved without schema migrations.
    try:
        return json.loads(row[0] or '{}')
    except Exception:
        return {}

@app.get('/health')
def health():
    configured = len(turso_catalog(include_disabled=True))
    return {'ok':True,'service':APP_NAME,'parser':'PY-RENDER-V9.5-DYNAMIC-DB-USAGE',
            'database':'Turso/libSQL','configured_databases':configured,'multi_account_ready':True,'max_pdf_mb':MAX_PDF_MB,
            'firebase_usage':'admin_auth_only'}

@app.get('/turso/list')
def turso_list(user=Depends(current_user)):
    return {'ok':True,'databases':[{k:v for k,v in x.items() if k not in {'url','token'}} for x in turso_catalog(True)]}

def _db_limit_gb(item):
    slot = int(item.get('slot') or 1)
    raw = os.getenv(f'TURSO_DATABASE_{slot}_LIMIT_GB', os.getenv('TURSO_DATABASE_LIMIT_GB', '5')).strip()
    try:
        value = float(raw)
        return value if value > 0 else 5.0
    except Exception:
        return 5.0


def _database_usage(item):
    """Return a lightweight storage indicator for one Turso/libSQL database.

    Primary method uses SQLite live pages: (page_count - freelist_count) * page_size.
    Deleted/free pages therefore stop counting as used capacity when SQLite puts them
    on the freelist. A content-size fallback is used if remote PRAGMA is unavailable.
    The limit defaults to 5 GiB per configured DB and can be overridden with
    TURSO_DATABASE_<N>_LIMIT_GB (or global TURSO_DATABASE_LIMIT_GB).
    """
    limit_gb = _db_limit_gb(item)
    limit_bytes = max(1, int(limit_gb * 1024 * 1024 * 1024))
    conn = connect_item(item, ensure=False)
    try:
        records = int(conn.execute('SELECT COUNT(*) FROM records').fetchone()[0])
        method = 'sqlite_live_pages'
        try:
            page_size = int(conn.execute('PRAGMA page_size').fetchone()[0])
            page_count = int(conn.execute('PRAGMA page_count').fetchone()[0])
            free_pages = int(conn.execute('PRAGMA freelist_count').fetchone()[0])
            live_pages = max(0, page_count - free_pages)
            used_bytes = max(0, live_pages * page_size)
            allocated_bytes = max(0, page_count * page_size)
        except Exception:
            method = 'live_content_estimate'
            raw_bytes = int(conn.execute("SELECT COALESCE(SUM(LENGTH(data_json)+LENGTH(record_key)),0) FROM records").fetchone()[0] or 0)
            # Allow practical headroom for table pages + indexes without pretending
            # this is Turso's billing meter.
            used_bytes = int(raw_bytes * 1.40)
            allocated_bytes = used_bytes
        percent = round(min(100.0, (used_bytes / limit_bytes) * 100.0), 3)
        return {
            'id': item['id'], 'name': item['name'], 'account': item.get('account',''),
            'slot': item.get('slot'), 'enabled': item.get('enabled', True),
            'records': records, 'used_bytes': used_bytes, 'allocated_bytes': allocated_bytes,
            'limit_bytes': limit_bytes, 'limit_gb': limit_gb, 'usage_percent': percent,
            'method': method,
        }
    finally:
        conn.close()


def _usage_snapshot():
    items = turso_catalog(True)
    databases=[]; errors=[]

    def one(item):
        try:
            return ('ok', _database_usage(item))
        except Exception as exc:
            return ('err', {'id':item['id'],'name':item['name'],'error':type(exc).__name__})

    if items:
        workers=max(1,min(len(items),5))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for kind, payload in pool.map(one, items):
                (databases if kind=='ok' else errors).append(payload)
    databases.sort(key=lambda x: int(x.get('slot') or 0))
    return {'ok':True,'databases':databases,'errors':errors,
            'default_limit_gb':float(os.getenv('TURSO_DATABASE_LIMIT_GB','5') or 5)}


def _stats_snapshot():
    items=turso_catalog(False)
    total=0; districts=set(); upazilas=set(); sources=[]; errors=[]

    def one(item):
        conn=None
        try:
            conn=connect_item(item, ensure=False)
            t=int(conn.execute('SELECT COUNT(*) FROM records').fetchone()[0])
            ds=[str(r[0]) for r in conn.execute(
                "SELECT DISTINCT district_name FROM records WHERE district_name<>''"
            ).fetchall()]
            us=[(str(r[0]),str(r[1])) for r in conn.execute(
                "SELECT DISTINCT district_name,upazila_name FROM records WHERE upazila_name<>''"
            ).fetchall()]
            return ('ok', item, t, ds, us)
        except Exception as exc:
            return ('err', item, type(exc).__name__, [], [])
        finally:
            if conn is not None:
                try: conn.close()
                except Exception: pass

    if items:
        workers=max(1,min(len(items),5))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for result in pool.map(one, items):
                if result[0]=='ok':
                    _, item, t, ds, us=result
                    total += t
                    districts.update(ds)
                    upazilas.update(us)
                    sources.append({'id':item['id'],'name':item['name'],'total':t,'slot':item.get('slot')})
                else:
                    _, item, err, _, _=result
                    errors.append({'id':item['id'],'name':item['name'],'error':err})
    sources.sort(key=lambda x: int(x.get('slot') or 0))
    for x in sources: x.pop('slot',None)
    return {'ok':True,'total':total,'districts':len(districts),'upazilas':len(upazilas),
            'sources':sources,'errors':errors}


def _cache_store(kind, data):
    with _METRICS_CACHE_LOCK:
        _METRICS_CACHE[kind]={'ts':time.time(),'data':data}
        _METRICS_REFRESHING[kind]=False

def _fast_cache_clear():
    with _METRICS_CACHE_LOCK:
        _METRICS_CACHE['stats'] = None
        _METRICS_CACHE['usage'] = None
        _METRICS_REFRESHING['stats'] = False
        _METRICS_REFRESHING['usage'] = False


def _refresh_metrics(kind):
    try:
        data = _stats_snapshot() if kind=='stats' else _usage_snapshot()
        _cache_store(kind, data)
    except Exception:
        with _METRICS_CACHE_LOCK:
            _METRICS_REFRESHING[kind]=False


def _start_metrics_refresh(kind):
    with _METRICS_CACHE_LOCK:
        if _METRICS_REFRESHING.get(kind):
            return
        _METRICS_REFRESHING[kind]=True
    try:
        Thread(target=_refresh_metrics,args=(kind,),name=f'{kind}-cache-refresh',daemon=True).start()
    except Exception:
        with _METRICS_CACHE_LOCK:
            _METRICS_REFRESHING[kind]=False


def _cached_metrics(kind):
    now=time.time()
    with _METRICS_CACHE_LOCK:
        item=_METRICS_CACHE.get(kind)
        refreshing=bool(_METRICS_REFRESHING.get(kind))
    if item:
        age=max(0.0,now-float(item.get('ts',0)))
        data=dict(item['data'])
        data['cache_age_seconds']=round(age,1)
        data['cached']=True
        if age <= DASHBOARD_CACHE_TTL:
            data['refreshing']=refreshing
            return data
        _start_metrics_refresh(kind)
        data['refreshing']=True
        data['stale']=True
        return data
    # First request after a cold start: query all DBs in parallel once.
    data = _stats_snapshot() if kind=='stats' else _usage_snapshot()
    _cache_store(kind, data)
    out=dict(data); out.update({'cached':False,'cache_age_seconds':0,'refreshing':False})
    return out


@app.get('/turso/usage-all')
def turso_usage_all(user=Depends(current_user)):
    return _cached_metrics('usage')


@app.get('/turso/stats-all')
def turso_stats_all(user=Depends(current_user)):
    return _cached_metrics('stats')


@app.get('/turso/areas')
def turso_areas(database_id:str='', user=Depends(current_user)):
    item=get_target(database_id); conn=connect_item(item)
    try:
        ds=[str(r[0]) for r in conn.execute("SELECT DISTINCT district_name FROM records WHERE district_name<>'' ORDER BY district_name").fetchall()]
        us=[{'district':str(r[0]),'upazila':str(r[1])} for r in conn.execute("SELECT DISTINCT district_name,upazila_name FROM records WHERE upazila_name<>'' ORDER BY district_name,upazila_name").fetchall()]
        return {'ok':True,'districts':ds,'upazilas':us}
    finally: conn.close()

@app.get('/turso/count')
def turso_count(database_id:str='', district:str='', upazila:str='', user=Depends(current_user)):
    item=get_target(database_id); conn=connect_item(item)
    try:
        sql='SELECT COUNT(*) FROM records WHERE 1=1'; args=[]
        if district: sql+=' AND district_name=?'; args.append(district)
        if upazila: sql+=' AND upazila_name=?'; args.append(upazila)
        return {'ok':True,'count':int(conn.execute(sql,args).fetchone()[0])}
    finally: conn.close()

@app.delete('/turso/records')
def turso_delete_records(database_id:str='', district:str='', upazila:str='', user=Depends(current_user)):
    if not district or not upazila: raise HTTPException(400,'জেলা ও উপজেলা প্রয়োজন')
    item=get_target(database_id); conn=connect_item(item)
    try:
        before=int(conn.execute('SELECT COUNT(*) FROM records WHERE district_name=? AND upazila_name=?',(district,upazila)).fetchone()[0])
        conn.execute('DELETE FROM records WHERE district_name=? AND upazila_name=?',(district,upazila)); conn.commit()
        return {'ok':True,'deleted':before}
    finally: conn.close()

@app.get('/public/search')
def public_search(district:str='',upazila:str='',name:str='',father:str='',mother:str='',village:str='',ward:str='', user=Depends(current_user)):
    """V21.5 fast area-routed search with Name/Parent + Village/Ward filters."""
    started=time.perf_counter()
    district=' '.join(str(district or '').split()); upazila=' '.join(str(upazila or '').split())
    name=unicodedata.normalize('NFC',' '.join(str(name or '').split()))
    father=unicodedata.normalize('NFC',' '.join(str(father or '').split()))
    mother=unicodedata.normalize('NFC',' '.join(str(mother or '').split()))
    village=unicodedata.normalize('NFC',' '.join(str(village or '').split()))
    ward=' '.join(str(ward or '').split())
    if not district or not upazila: raise HTTPException(400,'জেলা ও উপজেলা প্রয়োজন')
    all_items=turso_catalog(False)
    detail_count=sum(bool(x) for x in (name,father,mother,village,ward))

    area_items,route_cache_complete=_resolve_area_items(all_items,district,upazila)
    items_to_query=area_items
    if not items_to_query:
        return {'ok':True,'count':0,'results':[],'errors':[],
                'search_mode':'v21_2_area_empty','route_cache_hit':route_cache_complete,
                'databases_queried':0,'elapsed_ms':round((time.perf_counter()-started)*1000)}

    def phrase(value): return '"'+str(value).replace('"','""')+'"'
    person_query=_person_match_query(name,father,mother)

    def search_one(item):
        conn=None
        try:
            conn=connect_item(item, ensure=False)
            _probe_fts_ready_on_conn(conn,item['id'])
            person_ready=bool(person_query) and _person_fts_is_ready(item['id'])
            use_name=bool(name) and len(name)>=3 and _name_fts_is_ready(item['id'])
            use_aux=_aux_fts_is_ready(item['id']) and bool((father and len(father)>=3) or (mother and len(mother)>=3))
            use_geo=_geo_fts_is_ready(item['id']) and bool(village and len(village)>=3)

            sql='SELECT r.data_json FROM records AS r WHERE r.district_name=? AND r.upazila_name=?'; args=[district,upazila]
            if person_query and person_ready:
                sql+=' AND r.rowid IN (SELECT rowid FROM records_person_fts WHERE records_person_fts MATCH ?)'
                args.append(person_query)
            else:
                if name:
                    if use_name:
                        sql+=' AND r.rowid IN (SELECT rowid FROM records_name_fts WHERE name MATCH ?)'; args.append(phrase(name))
                    else:
                        sql+=' AND instr(r.name,?)>0'; args.append(name)
                if father:
                    if use_aux and len(father)>=3:
                        sql+=' AND r.rowid IN (SELECT rowid FROM records_aux_fts WHERE father_name MATCH ?)'; args.append(phrase(father))
                    else:
                        sql+=' AND instr(r.father_name,?)>0'; args.append(father)
                if mother:
                    if use_aux and len(mother)>=3:
                        sql+=' AND r.rowid IN (SELECT rowid FROM records_aux_fts WHERE mother_name MATCH ?)'; args.append(phrase(mother))
                    else:
                        sql+=' AND instr(r.mother_name,?)>0'; args.append(mother)

            if village:
                if use_geo and len(village)>=3:
                    sql+=' AND r.rowid IN (SELECT rowid FROM records_geo_fts WHERE records_geo_fts MATCH ?)'; args.append(phrase(village))
                else:
                    sql+=' AND (instr(r.address,?)>0 OR instr(r.voter_area,?)>0)'; args.extend([village,village])
            if ward:
                # V21.5: treat 1/01, 2/02 ... as the same ward without a table scan.
                # Keep an indexed IN lookup and support both ASCII and Bangla digits.
                ward_ascii=_ascii_digits(ward).strip()
                ward_variants=[]
                numeric_variants=[]
                if ward_ascii.isdigit():
                    n=str(int(ward_ascii))
                    numeric_variants.extend((n, n.zfill(2)))
                else:
                    numeric_variants.append(ward_ascii)
                for value in (ward, *numeric_variants):
                    value=str(value or '').strip()
                    if value and value not in ward_variants:
                        ward_variants.append(value)
                    bn=value.translate(_ASCII_TO_BN)
                    if bn and bn not in ward_variants:
                        ward_variants.append(bn)
                marks=','.join('?' for _ in ward_variants)
                sql+=f' AND r.ward_no IN ({marks})'
                args.extend(ward_variants)
            if not detail_count: sql+=' LIMIT 500'

            found=[]
            for raw in conn.execute(sql,args).fetchall():
                d=record_from_db(raw)
                d['_database_id']=item['id']; d['_database_name']=item['name']; found.append(d)
            indexed = person_ready or use_name or use_aux or use_geo or bool(ward)
            return found,None,{'database_id':item['id'],'indexed':bool(indexed),'person_token_index':bool(person_ready)}
        except Exception as exc:
            return [],{'database_id':item['id'],'error':type(exc).__name__},None
        finally:
            if conn is not None:
                try: conn.close()
                except Exception: pass

    def dedupe(rows):
        uniq={}
        for d in rows:
            key=str(d.get('voter_no') or '').strip() or '|'.join(str(d.get(k,'')).strip() for k in ('name','father_name','mother_name','birth_date','district_name','upazila_name'))
            if key not in uniq: uniq[key]=d
        return list(uniq.values())

    def one_rank(value,needle):
        if not needle: return 0
        value=unicodedata.normalize('NFC',' '.join(str(value or '').split()))
        needle=unicodedata.normalize('NFC',' '.join(str(needle or '').split()))
        if value==needle: return 0
        if value.startswith(needle): return 1
        if _token_value_matches(value,needle): return 2
        if needle in value: return 3
        return 20

    def result_rank(row):
        score=one_rank(row.get('name',''),name)+one_rank(row.get('father_name',''),father)+one_rank(row.get('mother_name',''),mother)
        if village: score += min(one_rank(row.get('address',''),village),one_rank(row.get('voter_area',''),village))
        score += one_rank(row.get('ward_no',''),ward)
        return (score,str(row.get('name') or ''),str(row.get('voter_no') or ''))

    rows=[]; errors=[]; diagnostics=[]
    workers=max(1,min(len(items_to_query),SEARCH_WORKERS))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures=[pool.submit(search_one,item) for item in items_to_query]
        for fut in as_completed(futures):
            found,err,diag=fut.result(); rows.extend(found)
            if err: errors.append(err)
            if diag: diagnostics.append(diag)
    rows=dedupe(rows); rows.sort(key=result_rank)
    all_indexed=bool(diagnostics) and all(x.get('indexed') for x in diagnostics)
    person_indexed=bool(person_query) and bool(diagnostics) and all(x.get('person_token_index') for x in diagnostics)
    mode='v21_4_person_token' if person_indexed else ('v21_4_area_indexed' if detail_count and all_indexed else ('v21_4_area_fallback' if detail_count else 'v21_4_area_only'))
    return {'ok':True,'count':len(rows),'results':rows,'errors':errors,'search_mode':mode,
            'route_cache_hit':route_cache_complete,'databases_queried':len(items_to_query),
            'elapsed_ms':round((time.perf_counter()-started)*1000)}

# PDF preview/upload post-processing.  Keep this deliberately conservative:
# parser.py remains the source extractor; these helpers only repair text/metadata
# already present in each parsed row before previewing or writing to Turso.
_PDF_TEXT_FIELDS = (
    'name','father_name','mother_name','profession','address','union_name',
    'post_office','voter_area','district_name','upazila_name','birth_date',
)
_PDF_DIGIT_FIELDS = ('voter_no','serial_no','post_code','voter_area_code','ward_no')
_PDF_LABEL_PREFIXES = {
    'name': ('নাম',),
    'father_name': ('পিতা', 'পিতার নাম'),
    'mother_name': ('মাতা', 'মাতার নাম'),
    'profession': ('পেশা',),
    'address': ('ঠিকানা', 'বর্তমান ঠিকানা'),
    'union_name': ('ইউনিয়ন', 'ইউনিয়ন'),
    'post_office': ('ডাকঘর',),
    'post_code': ('পোস্ট কোড', 'পোস্টকোড'),
    'voter_area': ('ভোটার এলাকার নাম', 'ভোটার এলাকা'),
    'voter_area_code': ('ভোটার এলাকার নং', 'ভোটার এলাকার নম্বর', 'ভোটার এলাকার কোড'),
    'ward_no': ('ওয়ার্ড নং', 'ওয়ার্ড নং', 'ওয়ার্ড নম্বর', 'ওয়ার্ড নম্বর'),
    'district_name': ('জেলা',),
    'upazila_name': ('উপজেলা', 'থানা'),
    'birth_date': ('জন্ম তারিখ', 'জন্মতারিখ', 'তারিখ জন্ম'),
}

# Detect known mojibake/control artifacts before repair_bangla() transforms them.
# Once transformed, a corrupted token can look like valid Bengali while carrying
# the wrong letters, so the original extracted value must be checked first.
_PDF_SUSPICIOUS_RE = re.compile(r'[\x80-\x9FËÎÏÐÑÒÔ×ØÙÚÌåêîïõøúûýÿĀăĐēĔėęĢĤĥĦħĨĩĮįıĲĳĴĽĺļńŇŌŐŘřŜśŝšŞŢŦũŬŮűŽžſƀƁƂƃƄƅƆƎƏƣŨūŋ¢µàŀ◌]')


# Conservative repair for PDF extraction that inserts a space inside one
# Bengali word (for example 'মোহাম্ম দ' or 'সুম ন').  Do not merge normal
# multi-word names: only a one-codepoint Bengali fragment is auto-joined,
# plus a tiny allow-list of very common split words seen in voter PDFs.
_PDF_KNOWN_BROKEN_SPACE_WORDS = {
    # Only exact, observed voter-PDF splits are auto-repaired.  Generic
    # one-letter joining is intentionally forbidden because it can mutate
    # legitimate names (e.g. "আবু ব কর" or "আলী ম").
    'বে গম': 'বেগম',
    'মোহাম্ম দ': 'মোহাম্মদ',
    'মো হাম্মদ': 'মোহাম্মদ',
    'সুম ন': 'সুমন',
    'রহি মা': 'রহিমা',
}

# Ambiguous split shapes that previously triggered a wrong auto-merge.  They
# are never guessed.  In critical identity fields they make the row unsafe so
# the original voter identity cannot be silently changed in the database.
_PDF_AMBIGUOUS_BROKEN_SPACE_RE = re.compile(
    r'(?:^|\s)(?:আবু\s+ব\s+কর|আলী\s+ম)(?:\s|$)'
)

def _pdf_repair_broken_spaces(value: str) -> str:
    x = str(value or '')
    for broken, fixed in _PDF_KNOWN_BROKEN_SPACE_WORDS.items():
        x = re.sub(r'(?<![ঀ-৿])' + re.escape(broken) + r'(?![ঀ-৿])', fixed, x)
    return re.sub(r'\s+', ' ', x).strip()

def _pdf_has_ambiguous_broken_space(value: str) -> bool:
    return bool(_PDF_AMBIGUOUS_BROKEN_SPACE_RE.search(str(value or '')))

# Common Bengali spellings of Latin-style initials and honorific fragments that
# may legitimately appear as short standalone tokens in a person's name.  They
# must not be mistaken for a PDF-inserted internal word break.
_PDF_ALLOWED_SHORT_IDENTITY_TOKENS = {
    'এ', 'বি', 'সি', 'ডি', 'ই', 'এফ', 'জি', 'এইচ', 'আই', 'জে', 'কে',
    'এল', 'এম', 'এন', 'ও', 'পি', 'কিউ', 'আর', 'এস', 'টি', 'ইউ',
    'ভি', 'এক্স', 'ওয়াই', 'ওয়াই', 'জেড',
    'মো', 'মোঃ', 'মো:', 'মিঃ', 'ডাঃ', 'আঃ', 'শ্রী',
}

def _pdf_bengali_base_count(token: str) -> int:
    # Count Bengali vowel/consonant bases, ignoring dependent vowel signs,
    # virama and other combining marks.  A split such as 'রহি মা' therefore
    # treats 'মা' as one base fragment even though it has two code points.
    return len(re.findall(r'[অ-ঔক-হড়-য়ৎ]', unicodedata.normalize('NFC', str(token or ''))))

def _pdf_short_identity_token(token: str) -> str:
    return re.sub(r'^[\s.,;:：()\[\]{}\-–—]+|[\s.,;:：()\[\]{}\-–—]+$', '', str(token or '')).strip()

def _pdf_has_unknown_broken_space(value: str) -> bool:
    """Detect unknown PDF-inserted spaces without guessing a repair.

    After exact known repairs have run, a standalone one-base Bengali fragment
    next to another Bengali token is highly suspicious in voter-name fields.
    Legitimate initials/honorifics are allow-listed.  We reject the row rather
    than concatenate unknown fragments, protecting the original identity.
    """
    x = _pdf_repair_broken_spaces(_pdf_clean_text_no_spacing_repair(value))
    tokens = [t for t in re.split(r'\s+', x) if t]
    if len(tokens) < 2:
        return False
    for i, raw in enumerate(tokens):
        token = _pdf_short_identity_token(raw)
        if not token or token in _PDF_ALLOWED_SHORT_IDENTITY_TOKENS:
            continue
        if _pdf_bengali_base_count(token) != 1:
            continue
        left = _pdf_short_identity_token(tokens[i-1]) if i > 0 else ''
        right = _pdf_short_identity_token(tokens[i+1]) if i + 1 < len(tokens) else ''
        left_bases = _pdf_bengali_base_count(left)
        right_bases = _pdf_bengali_base_count(right)
        if left_bases >= 2 or right_bases >= 2:
            return True
    return False

def _pdf_upload_critical_ok(row: dict) -> bool:
    # These fields identify the person and are expected on the voter-list PDFs
    # handled by this backend.  A blank/unsafe value is safer to reject than to
    # persist as a partial or potentially mis-associated voter record.
    for field in ('name', 'father_name', 'mother_name'):
        value = str(row.get(field) or '').strip()
        if not value or _PDF_SUSPICIOUS_RE.search(value):
            return False
    return True

def _pdf_clean_text_no_spacing_repair(value) -> str:
    x = repair_bangla(str(value or ''))
    x = clean_field(x)
    # Legacy extraction sometimes emits chandrabindu before aa-kar.
    x = x.replace('ঁা', 'াঁ')
    x = re.sub(r'\s+', ' ', x).strip()
    return unicodedata.normalize('NFC', x)

def _pdf_clean_text(value) -> str:
    x = _pdf_clean_text_no_spacing_repair(value)
    x = _pdf_repair_broken_spaces(x)
    return unicodedata.normalize('NFC', x)

def _pdf_strip_label(field: str, value: str) -> str:
    x = str(value or '').strip()
    # Match the most specific/longest label first.  Otherwise a value such as
    # 'পিতার নাম: ...' can be partially consumed by the shorter 'পিতা' label.
    labels = sorted(_PDF_LABEL_PREFIXES.get(field, ()), key=len, reverse=True)
    for label in labels:
        updated = re.sub(r'^\s*' + re.escape(label) + r'\s*[:：\-–—]?\s*', '', x, count=1, flags=re.I)
        if updated != x:
            x = updated
            break
    return x.strip()

def _pdf_normalize_person_prefix(value: str) -> str:
    x = str(value or '').strip()
    rules = (
        (r'^মোসা(?=\s|[:：.ঃ]|$)\s*[:：.ঃ]?\s*', 'মোসাঃ '),
        (r'^মোসা(?=\s|[:：.ঃ]|$)\s*[:：.ঃ]?\s*', 'মোসাঃ '),
        (r'^মুহা(?=\s|[:：.ঃ]|$)\s*[:：.ঃ]?\s*', 'মুহাঃ '),
        (r'^ডা(?=\s|[:：.ঃ]|$)\s*[:：.ঃ]?\s*', 'ডাঃ '),
        (r'^মো(?=\s|[:：.ঃ]|$)\s*[:：.ঃ]?\s*', 'মোঃ '),
        (r'^মো(?=\s|[:：.ঃ]|$)\s*[:：.ঃ]?\s*', 'মোঃ '),
    )
    for pat, repl in rules:
        if re.search(pat, x):
            x = re.sub(pat, repl, x, count=1)
            break
    return re.sub(r'\s+', ' ', x).strip()

def _pdf_digits(value: str, first_group: bool = False) -> str:
    # For compact IDs (voter/serial) separated digit fragments are intentional
    # and can be joined.  Metadata such as ward/post code must use only one
    # numeric token; joining every number can turn '৪ ... ২০১০' into '৪২০১০'.
    groups = re.findall(r'[0-9০-৯]+', str(value or ''))
    if not groups:
        return ''
    return groups[0] if first_group else ''.join(groups)

def _pdf_extract_field_number(field: str, value: str) -> str:
    src = _pdf_clean_text(value)
    patterns = {
        'ward_no': (
            r'(?:ওয়ার্ড|ওয়ার্ড)\s*(?:নং|নম্বর)?\s*[:：\-]?\s*([0-9০-৯]{1,3})',
        ),
        'post_code': (
            r'পোস্ট\s*কোড\s*[:：\-]?\s*([0-9০-৯]{4,6})',
        ),
        'voter_area_code': (
            r'ভোটার\s*এলাকার\s*(?:নং|নম্বর|কোড)\s*[:：\-]?\s*([0-9০-৯]+)',
        ),
    }
    for pat in patterns.get(field, ()):
        m = re.search(pat, src, re.I)
        if m:
            return _pdf_digits(m.group(1), first_group=True)
    return _pdf_digits(src, first_group=True)

def _pdf_clean_birth_date(value: str) -> str:
    src = _pdf_strip_label('birth_date', _pdf_clean_text(value))
    if not src:
        return ''

    # Keep a DOB only when it is a real calendar date.  A token that merely
    # looks date-like (for example 32/15/1990) is unsafe and must not reach DB.
    m = re.search(r'(?<![0-9০-৯])([0-9০-৯]{1,2}[./\-][0-9০-৯]{1,2}[./\-][0-9০-৯]{4})(?![0-9০-৯])', src)
    if m:
        token = m.group(1)
        ascii_token = token.translate(_BN_TO_ASCII)
        parts = re.split(r'[./\-]', ascii_token)
        try:
            day, month, year = (int(parts[0]), int(parts[1]), int(parts[2]))
            datetime(year, month, day)
            if 1900 <= year <= datetime.now().year:
                return token
        except Exception:
            return ''

    # Year-only values are accepted only when the whole cleaned field is that
    # year and it falls in a plausible range. Otherwise leave the field empty.
    compact = re.sub(r'\s+', '', src)
    if re.fullmatch(r'[0-9০-৯]{4}', compact):
        try:
            year = int(compact.translate(_BN_TO_ASCII))
            if 1900 <= year <= datetime.now().year:
                return compact
        except Exception:
            pass
    return ''

def _pdf_meta_pick(text: str, patterns) -> str:
    src = _pdf_clean_text(text)
    for pat in patterns:
        m = re.search(pat, src, re.I)
        if m:
            return _pdf_clean_text(m.group(1))
    return ''

def _pdf_infer_metadata(row: dict, district: str, upazila: str) -> dict:
    # Strict record-local inference only. Never use page-level raw/source text
    # here: one PDF page can contain multiple voters, so a page header/neighbor
    # number can silently attach the wrong ward/post/area code to this record.
    source = ' '.join(str(row.get(k) or '') for k in (
        'address','voter_area','post_office','union_name'
    ))
    source = repair_bangla(source)

    row['district_name'] = _pdf_clean_text(row.get('district_name') or district)
    row['upazila_name'] = _pdf_clean_text(row.get('upazila_name') or upazila)

    # Some legacy PDF extraction drops the final administrative suffix from
    # a post-office token (observed: 'ইসলামপুর' -> 'ইসলাম').  Repair only
    # when the truncated value exactly equals the selected/clean upazila name
    # with a known suffix removed.  This avoids guessing unrelated post offices.
    post_office = _pdf_clean_text(row.get('post_office') or '')
    upazila_clean = str(row.get('upazila_name') or '').strip()
    if post_office and upazila_clean:
        for suffix in ('পুর',):
            if upazila_clean.endswith(suffix) and post_office == upazila_clean[:-len(suffix)]:
                row['post_office'] = upazila_clean
                break

    if not str(row.get('ward_no') or '').strip():
        ward = _pdf_meta_pick(source, (
            r'(?:ওয়ার্ড|ওয়ার্ড)\s*(?:নং|নম্বর)\s*(?:[-–—:：]|\([^)]*\))?\s*([0-9০-৯]{1,3})',
            r'(?:ওয়াড|ওয়াড)\s*[^0-9০-৯]{0,8}([0-9০-৯]{1,3})',
        ))
        if ward:
            row['ward_no'] = _pdf_digits(ward)

    if not str(row.get('voter_area_code') or '').strip():
        val = _pdf_meta_pick(source, (
            r'ভোটার\s*এলাকার\s*(?:নং|নম্বর|কোড)\s*[:：\-]?\s*([0-9০-৯]+)',
        ))
        if val:
            row['voter_area_code'] = _pdf_digits(val)

    if not str(row.get('voter_area') or '').strip():
        val = _pdf_meta_pick(source, (
            r'ভোটার\s*এলাকার\s*নাম\s*[:：]\s*(.*?)(?=\s+(?:ভোটার\s*এলাকার|ওয়ার্ড|ওয়ার্ড|ডাকঘর|পোস্ট\s*কোড|$))',
        ))
        if val:
            row['voter_area'] = val

    if not str(row.get('post_code') or '').strip():
        val = _pdf_meta_pick(source, (r'পোস্ট\s*কোড\s*[:：\-]?\s*([0-9০-৯]{4})',))
        if val:
            row['post_code'] = _pdf_digits(val)

    if not str(row.get('post_office') or '').strip():
        val = _pdf_meta_pick(source, (r'ডাকঘর\s*[:：]\s*(.*?)(?=\s+(?:পোস্ট\s*কোড|ভোটার|ওয়ার্ড|ওয়ার্ড|$))',))
        if val:
            row['post_office'] = val

    if not str(row.get('union_name') or '').strip():
        val = _pdf_meta_pick(source, (r'ইউনিয়ন\s*[:：]\s*(.*?)(?=\s+(?:ডাকঘর|পোস্ট\s*কোড|ভোটার|ওয়ার্ড|ওয়ার্ড|$))',))
        if val:
            row['union_name'] = val
    return row

def sanitize_pdf_record(record: dict, district: str = '', upazila: str = '') -> dict:
    row = dict(record or {})
    changed = False
    unsafe_fields = []

    for field in _PDF_TEXT_FIELDS:
        old = str(row.get(field) or '')
        # Apply broken-text safety to every textual voter field, not only
        # person/address fields. A mojibake district/upazila/union/post office
        # is still unsafe data and must not reach preview/database output.
        if field != 'birth_date' and _PDF_SUSPICIOUS_RE.search(old):
            unsafe_fields.append(field)
        # Never guess ambiguous internal spacing in identity fields. Exact
        # known PDF splits are repaired; ambiguous shapes are rejected.
        if field in ('name','father_name','mother_name') and (
            _pdf_has_ambiguous_broken_space(old) or _pdf_has_unknown_broken_space(old)
        ):
            unsafe_fields.append(field)
        if field == 'birth_date':
            new = _pdf_clean_birth_date(old)
        else:
            new = _pdf_strip_label(field, _pdf_clean_text(old))
        if field in ('name','father_name','mother_name'):
            new = _pdf_normalize_person_prefix(new)
        if field == 'profession':
            new = re.sub(r'^(?:গৃহিনী|গৃহীনি|গিহিনী)$', 'গৃহিণী', new)
        if field == 'address':
            new = re.sub(r'\s*(?:চূড়ান্ত|চূড়ান্ত|ন্ত)?\s*ভোটার\s*তালিকা\s*', ' ', new)
            new = re.sub(r'\s*রেজিস্ট্রেশন\s*অফিসার\s*$', '', new)
            new = re.sub(r'\s*,\s*', ', ', new)
            new = re.sub(r'\s{2,}', ' ', new).strip(' ,;')
        if new != old:
            changed = True
        row[field] = new

    for field in _PDF_DIGIT_FIELDS:
        old = str(row.get(field) or '')
        stripped = _pdf_strip_label(field, _pdf_clean_text(old))
        if field in ('ward_no','post_code','voter_area_code'):
            new = _pdf_extract_field_number(field, old)
        else:
            new = _pdf_digits(stripped, first_group=False)
        # Do not erase voter/serial values if a rare parser version stores a
        # non-numeric key. Other metadata fields are expected to be numeric.
        if field in ('voter_no','serial_no') and old and not new:
            new = stripped
        if new != old:
            changed = True
        row[field] = new

    # Clear unsafe text BEFORE metadata inference. Otherwise a broken address
    # can still donate a plausible-looking ward/post/area number to the record
    # before the bad text itself is removed later. Form district/upazila values
    # remain available as trusted fallbacks inside _pdf_infer_metadata().
    for field in dict.fromkeys(unsafe_fields):
        if str(row.get(field) or ''):
            row[field] = ''
            changed = True

    before_meta = {k: row.get(k,'') for k in ('district_name','upazila_name','union_name','post_office','post_code','voter_area','voter_area_code','ward_no')}
    row = _pdf_infer_metadata(row, district, upazila)
    after_meta = {k: row.get(k,'') for k in before_meta}
    if before_meta != after_meta:
        changed = True

    # Rebuild the fallback key only if parser could not provide one. Never
    # change a voter-based key, which protects existing upsert behaviour.
    if not str(row.get('record_key') or '').strip():
        voter = str(row.get('voter_no') or '').strip()
        row['record_key'] = f'v:{voter}' if voter else 's:' + '|'.join(
            str(row.get(k) or '').strip() for k in ('district_name','upazila_name','source_file','serial_no')
        )
        changed = True

    warning_fields = []
    for field in tuple(f for f in _PDF_TEXT_FIELDS if f != 'birth_date'):
        value = str(row.get(field) or '')
        original_unsafe = field in unsafe_fields
        current_unsafe = bool(_PDF_SUSPICIOUS_RE.search(value))
        # A broken district/upazila may be replaced above by the trusted form
        # selection. Preserve that clean fallback while still rejecting any
        # suspicious value that remains after inference.
        if current_unsafe or (original_unsafe and field not in ('district_name','upazila_name')):
            warning_fields.append(field)
            # Do not preserve visibly corrupted text into preview/upload rows.
            # Empty is safer than writing fabricated/wrong voter information.
            row[field] = ''
            changed = True
    row['parser_warning_text'] = ','.join(dict.fromkeys(warning_fields))
    if warning_fields:
        row['parse_status'] = 'sanitized_broken_text_removed'
    elif changed and row.get('parse_status') == 'raw_preserved':
        row['parse_status'] = 'sanitized'
    elif not row.get('parse_status'):
        row['parse_status'] = 'clean'
    row['upload_eligible'] = _pdf_upload_critical_ok(row)
    if not row['upload_eligible']:
        existing = [x for x in str(row.get('parser_warning_text') or '').split(',') if x]
        existing.append('critical_identity_missing')
        row['parser_warning_text'] = ','.join(dict.fromkeys(existing))
        row['parse_status'] = 'rejected_critical_missing'
    row['sanitized_main_py'] = True
    return row

def sanitize_pdf_rows(rows, district: str = '', upazila: str = ''):
    return [sanitize_pdf_record(r, district, upazila) for r in (rows or [])]

async def read_pdf(file:UploadFile):
    data=await file.read()
    if not data: raise HTTPException(400,'PDF file খালি')
    if len(data)>MAX_PDF_MB*1024*1024: raise HTTPException(413,f'PDF সর্বোচ্চ {MAX_PDF_MB} MB হতে পারবে')
    return data

@app.post('/preview')
async def preview(district:str=Form(...),upazila:str=Form(...),file:UploadFile=File(...),user=Depends(current_user)):
    data=await read_pdf(file)
    district = district.strip(); upazila = upazila.strip()
    cache_key = _parse_cache_key(data, district, upazila)
    try: rows=parse_pdf_bytes(data,district,upazila,file.filename)
    except Exception as e: raise HTTPException(422,f'PDF parse করা যায়নি: {e}') from e
    if not rows: raise HTTPException(422,'PDF থেকে কোনো Record শনাক্ত করা যায়নি')
    rows=sanitize_pdf_rows(rows,district,upazila)
    _cache_put(cache_key, rows)
    raw=sum(1 for r in rows if r.get('parse_status')=='raw_preserved')
    return {'ok':True,'records_detected':len(rows),'raw_preserved':raw,'preview':rows[:20],
            'parser':'PY-RENDER-V9.8-MAIN-SANITIZE-PREVIEW','upload_cache_ready':True}

@app.post('/upload')
async def upload(district:str=Form(...),upazila:str=Form(...),database_id:str=Form(''),file:UploadFile=File(...),user=Depends(current_user)):
    data=await read_pdf(file)
    district = district.strip(); upazila = upazila.strip()
    cache_key = _parse_cache_key(data, district, upazila)
    rows = _cache_get(cache_key)
    cache_hit = rows is not None
    if rows is None:
        try: rows=parse_pdf_bytes(data,district,upazila,file.filename)
        except Exception as e: raise HTTPException(422,f'PDF parse করা যায়নি: {e}') from e
    if not rows: raise HTTPException(422,'PDF থেকে কোনো Record শনাক্ত করা যায়নি')
    rows=sanitize_pdf_rows(rows,district,upazila)
    detected_rows = len(rows)
    rejected_rows = [r for r in rows if not r.get('upload_eligible', True)]
    rows = [r for r in rows if r.get('upload_eligible', True)]
    if not rows:
        raise HTTPException(422,'কোনো নিরাপদ Record পাওয়া যায়নি; নাম/পিতা/মাতার তথ্য অসম্পূর্ণ বা ভাঙা')
    item=get_target(database_id)
    raw=sum(1 for r in rows if r.get('parse_status')=='raw_preserved')
    now=datetime.now(timezone.utc).isoformat()
    parser_name='PY-RENDER-V9.8-MAIN-SANITIZE-UPLOAD'
    import_meta={
        'database_id':item['id'], 'district_name':district, 'upazila_name':upazila,
        'file_name':file.filename, 'records_detected':detected_rows, 'created_at':now,
        'uploaded_by':user.get('email',''), 'parser':parser_name,
    }
    started=time.perf_counter()
    try: write_result=write_rows(rows,item,import_meta=import_meta)
    except Exception as e: raise HTTPException(500,f'Turso write failed: {type(e).__name__}: {e}') from e
    upload_seconds=round(time.perf_counter()-started,3)
    _route_add(district, upazila, item['id'])
    # Usage/stats caches are now stale after a successful write. Clear them so
    # the next dashboard refresh is correct without slowing the upload itself.
    try:
        _fast_cache_clear()
    except Exception:
        pass
    written=int(write_result['records_written'])
    log={'database_id':item['id'],'database_name':item['name'],'district_name':district,'upazila_name':upazila,
         'file_name':file.filename,'records_detected':detected_rows,'records_rejected_unsafe':len(rejected_rows),'records_written':written,
         'batch_commits':write_result['batch_commits'],
         'records_added':write_result['records_added'],'records_updated':write_result['records_updated'],
         'records_unchanged':write_result['records_unchanged'],'records_skipped':write_result['records_skipped'],
         'duplicate_input_keys':write_result['duplicate_input_keys'],
         'records_added_or_updated':write_result['records_added']+write_result['records_updated'],
         'raw_preserved':raw,'created_at':now,'uploaded_by':user.get('email',''),
         'preview_cache_hit':cache_hit,'upload_db_seconds':upload_seconds,
         'parser':parser_name}
    return {'ok':True,**log}

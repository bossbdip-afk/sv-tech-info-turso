import hashlib, json, os, re, time, threading
import urllib.error, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import libsql
import firebase_admin
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from firebase_admin import auth, credentials

from .parser import parse_pdf_bytes

APP_NAME = 'SV Tech Multi-Turso PDF Backend'
MAX_PDF_MB = int(os.getenv('MAX_PDF_MB', '100'))

# Preview -> upload usually happens immediately with the same PDF.  Cache parsed
# rows briefly so upload does not parse every PDF a second time.  The cache is
# best-effort only: a cold restart or another worker simply falls back to parsing.
PREVIEW_CACHE_TTL = int(os.getenv('PREVIEW_CACHE_TTL_SECONDS', '900'))
PREVIEW_CACHE_MAX = int(os.getenv('PREVIEW_CACHE_MAX_ITEMS', '6'))
_PREVIEW_CACHE = {}

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

# Warm-search acceleration. Render commonly keeps one process alive for many
# requests, so reusing worker threads and their read-only Turso connections
# avoids paying connection setup on every search.
SEARCH_CACHE_TTL = int(os.getenv('SEARCH_CACHE_TTL_SECONDS', '120'))
SEARCH_CACHE_MAX = int(os.getenv('SEARCH_CACHE_MAX_ITEMS', '256'))
AREA_ROUTE_TTL = int(os.getenv('AREA_ROUTE_TTL_SECONDS', '3600'))
_SEARCH_CACHE = {}
_AREA_DB_CACHE = {}
_SEARCH_LOCK = threading.Lock()
_SEARCH_LOCAL = threading.local()
_SEARCH_POOL = ThreadPoolExecutor(max_workers=max(2, int(os.getenv('SEARCH_WORKERS', '8'))))
_SCHEMA_READY = set()
_SCHEMA_LOCK = threading.Lock()

def _clean_search_part(v):
    return re.sub(r'\s+', ' ', str(v or '').strip()).casefold()

def _search_cache_key(district, upazila, name, father, mother, dob):
    return '|'.join(_clean_search_part(x) for x in (district, upazila, name, father, mother, dob))

def _search_cache_get(key):
    now=time.time()
    with _SEARCH_LOCK:
        item=_SEARCH_CACHE.get(key)
        if not item: return None
        if now-item['ts']>SEARCH_CACHE_TTL:
            _SEARCH_CACHE.pop(key,None); return None
        return item['value']

def _search_cache_put(key, value):
    now=time.time()
    with _SEARCH_LOCK:
        _SEARCH_CACHE[key]={'ts':now,'value':value}
        if len(_SEARCH_CACHE)>SEARCH_CACHE_MAX:
            oldest=min(_SEARCH_CACHE,key=lambda k:_SEARCH_CACHE[k]['ts'])
            _SEARCH_CACHE.pop(oldest,None)

def _area_route_get(district, upazila):
    key=(_clean_search_part(district),_clean_search_part(upazila)); now=time.time()
    with _SEARCH_LOCK:
        item=_AREA_DB_CACHE.get(key)
        if not item: return None
        if now-item['ts']>AREA_ROUTE_TTL:
            _AREA_DB_CACHE.pop(key,None); return None
        return set(item['ids'])

def _area_route_note(district, upazila, database_id):
    key=(_clean_search_part(district),_clean_search_part(upazila)); now=time.time()
    with _SEARCH_LOCK:
        item=_AREA_DB_CACHE.setdefault(key,{'ts':now,'ids':set()})
        item['ts']=now; item['ids'].add(str(database_id))
        # Any write to this area may change prior cached search results.
        for k in list(_SEARCH_CACHE):
            if k.startswith(key[0]+'|'+key[1]+'|'):
                _SEARCH_CACHE.pop(k,None)

def _thread_search_conn(item):
    conns=getattr(_SEARCH_LOCAL,'conns',None)
    if conns is None:
        conns={}; _SEARCH_LOCAL.conns=conns
    dbid=str(item['id'])
    conn=conns.get(dbid)
    if conn is None:
        conn=connect_item(item, ensure=False); conns[dbid]=conn
    return conn

def _drop_thread_search_conn(item):
    conns=getattr(_SEARCH_LOCAL,'conns',None) or {}
    conn=conns.pop(str(item['id']),None)
    if conn is not None:
        try: conn.close()
        except Exception: pass
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

app = FastAPI(title=APP_NAME, version='9.11.0')
origins = [x.strip() for x in os.getenv('ALLOWED_ORIGINS','*').split(',') if x.strip()]
app.add_middleware(CORSMiddleware, allow_origins=origins or ['*'], allow_credentials=False,
                   allow_methods=['GET','POST','DELETE','OPTIONS'], allow_headers=['*'])

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
    # CREATE TABLE/INDEX IF NOT EXISTS is safe but expensive over a remote DB.
    # Ensure once per database per warm process, not on every upload/log write.
    if ensure and str(item['id']) not in _SCHEMA_READY:
        with _SCHEMA_LOCK:
            if str(item['id']) not in _SCHEMA_READY:
                try:
                    conn.execute('SELECT 1 FROM records LIMIT 1').fetchone()
                except Exception:
                    ensure_schema(conn)
                _SCHEMA_READY.add(str(item['id']))
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


def _bulk_upsert(conn, params_rows, batch_size=25):
    """Send many records per SQL statement instead of one remote execute per row."""
    if not params_rows: return 0
    columns = ('record_key','voter_no','serial_no','name','father_name','mother_name','profession','birth_date','district_name','upazila_name','address','union_name','post_office','post_code','voter_area','voter_area_code','ward_no','source_file','created_at','data_json')
    update = ('voter_no=excluded.voter_no,serial_no=excluded.serial_no,name=excluded.name,father_name=excluded.father_name,'
              'mother_name=excluded.mother_name,profession=excluded.profession,birth_date=excluded.birth_date,'
              'district_name=excluded.district_name,upazila_name=excluded.upazila_name,address=excluded.address,'
              'union_name=excluded.union_name,post_office=excluded.post_office,post_code=excluded.post_code,'
              'voter_area=excluded.voter_area,voter_area_code=excluded.voter_area_code,ward_no=excluded.ward_no,'
              'source_file=excluded.source_file,created_at=excluded.created_at,data_json=excluded.data_json')
    batches=0
    one='('+','.join('?' for _ in columns)+')'
    for start in range(0,len(params_rows),batch_size):
        chunk=params_rows[start:start+batch_size]
        sql='INSERT INTO records ('+','.join(columns)+') VALUES '+','.join(one for _ in chunk)+' ON CONFLICT(record_key) DO UPDATE SET '+update
        flat=[value for row in chunk for value in row]
        conn.execute(sql,flat); batches+=1
    return batches

def write_rows(rows, item):
    conn = connect_item(item)
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

        write_batches=0
        if to_write:
            # Multi-row UPSERT cuts remote Turso round-trips dramatically versus
            # DB-API executemany on large voter PDFs. One commit keeps it atomic.
            write_batches=_bulk_upsert(conn,to_write)
            conn.commit()

        return {
            'records_added': added,
            'records_updated': updated,
            'records_unchanged': unchanged,
            'records_skipped': 0,
            'duplicate_input_keys': duplicate_keys,
            'records_written': added + updated,
            'batch_commits': 1 if to_write else 0,
            'write_batches': write_batches,
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
    return {'ok':True,'service':APP_NAME,'parser':'PY-RENDER-V9.11-BOUNDED-HTTP-SEARCH',
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
    conn = connect_item(item)
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


@app.get('/turso/usage-all')
def turso_usage_all(user=Depends(current_user)):
    databases=[]; errors=[]
    for item in turso_catalog(True):
        try:
            databases.append(_database_usage(item))
        except Exception as exc:
            errors.append({'id':item['id'],'name':item['name'],'error':type(exc).__name__})
    return {'ok':True,'databases':databases,'errors':errors,'default_limit_gb':float(os.getenv('TURSO_DATABASE_LIMIT_GB','5') or 5)}


@app.get('/turso/stats-all')
def turso_stats_all(user=Depends(current_user)):
    total=0; districts=set(); upazilas=set(); sources=[]; errors=[]
    for item in turso_catalog(False):
        try:
            conn=connect_item(item)
            t=int(conn.execute('SELECT COUNT(*) FROM records').fetchone()[0])
            for r in conn.execute("SELECT DISTINCT district_name FROM records WHERE district_name<>''").fetchall(): districts.add(str(r[0]))
            for r in conn.execute("SELECT DISTINCT district_name,upazila_name FROM records WHERE upazila_name<>''").fetchall(): upazilas.add((str(r[0]),str(r[1])))
            conn.close(); total += t
            sources.append({'id':item['id'],'name':item['name'],'total':t})
        except Exception as exc:
            errors.append({'id':item['id'],'name':item['name'],'error':type(exc).__name__})
    return {'ok':True,'total':total,'districts':len(districts),'upazilas':len(upazilas),'sources':sources,'errors':errors}

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
def public_search(district:str='',upazila:str='',name:str='',father:str='',mother:str='',dob:str='', user=Depends(current_user)):
    district=district.strip(); upazila=upazila.strip()
    if not district or not upazila: raise HTTPException(400,'জেলা ও উপজেলা প্রয়োজন')
    items=turso_catalog(False)
    started=time.monotonic()

    # One search query is a single round-trip, so Turso's HTTP protocol is a
    # better fit than opening a remote SDK connection for every database.  Most
    # importantly, urllib gives us a real socket timeout: a slow/unreachable DB
    # can no longer hold the whole endpoint for 1-2 minutes.
    per_db_timeout=max(1.0, min(float(os.getenv('SEARCH_DB_TIMEOUT_SECONDS','8')), 20.0))

    def http_url(item):
        url=str(item.get('url') or '').strip().rstrip('/')
        if url.startswith('libsql://'):
            url='https://'+url[len('libsql://'):]
        elif url.startswith('turso://'):
            url='https://'+url[len('turso://'):]
        elif url.startswith('http://') or url.startswith('https://'):
            pass
        else:
            url='https://'+url
        return url+'/v2/pipeline'

    def text_arg(value):
        return {'type':'text','value':str(value)}

    def search_one(item):
        db_started=time.monotonic()
        try:
            sql='SELECT data_json FROM records WHERE district_name=? AND upazila_name=?'; args=[district,upazila]
            for col,val in [('name',name),('father_name',father),('mother_name',mother),('birth_date',dob)]:
                val=val.strip()
                if val:
                    sql += f' AND {col} LIKE ?'; args.append('%'+val+'%')
            sql += ' LIMIT 500'
            payload=json.dumps({'requests':[
                {'type':'execute','stmt':{'sql':sql,'args':[text_arg(x) for x in args]}},
                {'type':'close'}
            ]}, ensure_ascii=False).encode('utf-8')
            req=urllib.request.Request(
                http_url(item), data=payload, method='POST',
                headers={'Authorization':'Bearer '+str(item['token']), 'Content-Type':'application/json'}
            )
            with urllib.request.urlopen(req, timeout=per_db_timeout) as resp:
                body=json.loads(resp.read().decode('utf-8'))
            result0=(body.get('results') or [{}])[0]
            if result0.get('type')!='ok':
                err=result0.get('error') or {}
                raise RuntimeError(str(err.get('message') or 'Turso query failed'))
            result=((result0.get('response') or {}).get('result') or {})
            found=[]
            for row in result.get('rows') or []:
                cell=row[0] if row else None
                raw=cell.get('value') if isinstance(cell,dict) else cell
                d=record_from_db((raw,)); d['_database_id']=item['id']; d['_database_name']=item['name']; found.append(d)
            return found, None, int((time.monotonic()-db_started)*1000)
        except Exception as exc:
            return [], {'database_id':item['id'],'error':type(exc).__name__}, int((time.monotonic()-db_started)*1000)

    rows=[]; errors=[]; timings={}
    # Each worker has a hard network timeout, so waiting for all workers is now
    # bounded instead of being controlled by the slowest libSQL connection.
    workers=max(1,min(len(items),5))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map={pool.submit(search_one,item):item for item in items}
        for fut in as_completed(future_map):
            item=future_map[fut]
            try:
                found,err,elapsed=fut.result()
            except Exception as exc:
                found=[]; err={'database_id':item['id'],'error':type(exc).__name__}; elapsed=int((time.monotonic()-started)*1000)
            rows.extend(found); timings[str(item['id'])]=elapsed
            if err: errors.append(err)

    uniq={}
    for d in rows:
        key=str(d.get('voter_no') or '').strip() or '|'.join(str(d.get(k,'')).strip() for k in ('name','father_name','birth_date','district_name','upazila_name'))
        if key not in uniq: uniq[key]=d
    return {'ok':True,'count':len(uniq),'results':list(uniq.values()),'errors':errors,
            'search_ms':int((time.monotonic()-started)*1000),'database_ms':timings,
            'database_timeout_seconds':per_db_timeout}

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
    _cache_put(cache_key, rows)
    raw=sum(1 for r in rows if r.get('parse_status')=='raw_preserved')
    return {'ok':True,'records_detected':len(rows),'raw_preserved':raw,'preview':rows[:20],
            'parser':'PY-RENDER-V9.8-WARD-UNICODE-FAST','upload_cache_ready':True,'preview_token':cache_key}

@app.post('/upload')
async def upload(district:str=Form(...),upazila:str=Form(...),database_id:str=Form(''),preview_token:str=Form(''),file_name:str=Form(''),file:UploadFile|None=File(None),user=Depends(current_user)):
    district = district.strip(); upazila = upazila.strip(); preview_token=preview_token.strip()
    rows = _cache_get(preview_token) if preview_token else None
    cache_hit = rows is not None
    source_name=(file_name or (file.filename if file else '') or 'upload.pdf').strip()
    if rows is not None:
        first=rows[0] if rows else {}
        if str(first.get('district_name','')).strip()!=district or str(first.get('upazila_name','')).strip()!=upazila:
            rows=None; cache_hit=False
    if rows is None:
        if file is None:
            raise HTTPException(409,'Preview cache মেয়াদ শেষ হয়েছে। PDF আবার পাঠাতে হবে।')
        data=await read_pdf(file)
        cache_key = _parse_cache_key(data, district, upazila)
        rows = _cache_get(cache_key)
        cache_hit = rows is not None
        if rows is None:
            try: rows=parse_pdf_bytes(data,district,upazila,file.filename)
            except Exception as e: raise HTTPException(422,f'PDF parse করা যায়নি: {e}') from e
        source_name=file.filename or source_name
    if not rows: raise HTTPException(422,'PDF থেকে কোনো Record শনাক্ত করা যায়নি')
    item=get_target(database_id)
    try: write_result=write_rows(rows,item)
    except Exception as e: raise HTTPException(500,f'Turso write failed: {type(e).__name__}: {e}') from e
    raw=sum(1 for r in rows if r.get('parse_status')=='raw_preserved')
    now=datetime.now(timezone.utc).isoformat()
    written=int(write_result['records_written'])
    log={'database_id':item['id'],'database_name':item['name'],'district_name':district,'upazila_name':upazila,
         'file_name':source_name,'records_detected':len(rows),'records_written':written,
         'batch_commits':write_result['batch_commits'],'write_batches':write_result.get('write_batches',0),
         'records_added':write_result['records_added'],'records_updated':write_result['records_updated'],
         'records_unchanged':write_result['records_unchanged'],'records_skipped':write_result['records_skipped'],
         'duplicate_input_keys':write_result['duplicate_input_keys'],
         'records_added_or_updated':write_result['records_added']+write_result['records_updated'],
         'raw_preserved':raw,'created_at':now,'uploaded_by':user.get('email',''),
         'preview_cache_hit':cache_hit,
         'parser':'PY-RENDER-V9.8-WARD-UNICODE-UPLOAD-FAST'}
    _area_route_note(district,upazila,item['id'])
    conn=connect_item(item, ensure=False)
    try:
        iid='import_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
        conn.execute('INSERT INTO pdf_imports(id,database_id,district_name,upazila_name,file_name,records_detected,records_written,created_at,uploaded_by,parser) VALUES (?,?,?,?,?,?,?,?,?,?)',
                     (iid,item['id'],district,upazila,source_name,len(rows),written,now,user.get('email',''),log['parser']))
        conn.commit()
    finally: conn.close()
    return {'ok':True,**log}

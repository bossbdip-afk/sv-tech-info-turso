import hashlib, json, os
from datetime import datetime, timezone

import libsql
import firebase_admin
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from firebase_admin import auth, credentials

from .parser import parse_pdf_bytes

APP_NAME = 'SV Tech Multi-Turso PDF Backend'
MAX_PDF_MB = int(os.getenv('MAX_PDF_MB', '100'))
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

app = FastAPI(title=APP_NAME, version='9.0.0')
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
    # Recommended setup: TURSO_DATABASE_1_URL ... TURSO_DATABASE_5_URL
    for i in range(1, 21):
        url = os.getenv(f'TURSO_DATABASE_{i}_URL','').strip()
        token = os.getenv(f'TURSO_DATABASE_{i}_AUTH_TOKEN','').strip()
        if not url or not token:
            continue
        enabled = os.getenv(f'TURSO_DATABASE_{i}_ENABLED','true').strip().lower() not in {'0','false','no','off'}
        if not include_disabled and not enabled:
            continue
        name = os.getenv(f'TURSO_DATABASE_{i}_NAME', f'Turso DB {i}').strip() or f'Turso DB {i}'
        dbid = os.getenv(f'TURSO_DATABASE_{i}_ID', f'db{i}').strip() or f'db{i}'
        items.append({'id': clean_id(dbid), 'name': name, 'url': url, 'token': token, 'enabled': enabled, 'slot': i})
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


def connect_item(item):
    conn = libsql.connect(database=item['url'], auth_token=item['token'])
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


def write_rows(rows, item):
    conn = connect_item(item)
    try:
        # sqlite-compatible DB-API; executemany keeps the upload far cheaper than one network commit per row.
        conn.executemany(UPSERT_SQL, [row_params(r) for r in rows])
        conn.commit()
        return len(rows), 1
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
    return {'ok':True,'service':APP_NAME,'parser':'PY-RENDER-V9-MULTI-TURSO',
            'database':'Turso/libSQL','configured_databases':configured,'max_pdf_mb':MAX_PDF_MB,
            'firebase_usage':'admin_auth_only'}

@app.get('/turso/list')
def turso_list(user=Depends(current_user)):
    return {'ok':True,'databases':[{k:v for k,v in x.items() if k not in {'url','token'}} for x in turso_catalog(True)]}

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
def public_search(district:str='',upazila:str='',name:str='',father:str='',mother:str='',dob:str=''):
    district=district.strip(); upazila=upazila.strip()
    if not district or not upazila: raise HTTPException(400,'জেলা ও উপজেলা প্রয়োজন')
    rows=[]; errors=[]
    for item in turso_catalog(False):
        try:
            conn=connect_item(item)
            sql='SELECT data_json FROM records WHERE district_name=? AND upazila_name=?'; args=[district,upazila]
            for col,val in [('name',name),('father_name',father),('mother_name',mother),('birth_date',dob)]:
                val=val.strip()
                if val:
                    sql += f' AND {col} LIKE ?'; args.append('%'+val+'%')
            sql += ' LIMIT 500'
            for raw in conn.execute(sql,args).fetchall():
                d=record_from_db(raw); d['_database_id']=item['id']; d['_database_name']=item['name']; rows.append(d)
            conn.close()
        except Exception as exc:
            errors.append({'database_id':item['id'],'error':type(exc).__name__})
    uniq={}
    for d in rows:
        key=str(d.get('voter_no') or '').strip() or '|'.join(str(d.get(k,'')).strip() for k in ('name','father_name','birth_date','district_name','upazila_name'))
        if key not in uniq: uniq[key]=d
    return {'ok':True,'count':len(uniq),'results':list(uniq.values()),'errors':errors}

async def read_pdf(file:UploadFile):
    data=await file.read()
    if not data: raise HTTPException(400,'PDF file খালি')
    if len(data)>MAX_PDF_MB*1024*1024: raise HTTPException(413,f'PDF সর্বোচ্চ {MAX_PDF_MB} MB হতে পারবে')
    return data

@app.post('/preview')
async def preview(district:str=Form(...),upazila:str=Form(...),file:UploadFile=File(...),user=Depends(current_user)):
    data=await read_pdf(file)
    try: rows=parse_pdf_bytes(data,district.strip(),upazila.strip(),file.filename)
    except Exception as e: raise HTTPException(422,f'PDF parse করা যায়নি: {e}') from e
    if not rows: raise HTTPException(422,'PDF থেকে কোনো Record শনাক্ত করা যায়নি')
    raw=sum(1 for r in rows if r.get('parse_status')=='raw_preserved')
    return {'ok':True,'records_detected':len(rows),'raw_preserved':raw,'preview':rows[:20],'parser':'PY-RENDER-V9-MULTI-TURSO'}

@app.post('/upload')
async def upload(district:str=Form(...),upazila:str=Form(...),database_id:str=Form(''),file:UploadFile=File(...),user=Depends(current_user)):
    data=await read_pdf(file)
    try: rows=parse_pdf_bytes(data,district.strip(),upazila.strip(),file.filename)
    except Exception as e: raise HTTPException(422,f'PDF parse করা যায়নি: {e}') from e
    if not rows: raise HTTPException(422,'PDF থেকে কোনো Record শনাক্ত করা যায়নি')
    item=get_target(database_id)
    try: written,batches=write_rows(rows,item)
    except Exception as e: raise HTTPException(500,f'Turso write failed: {type(e).__name__}: {e}') from e
    raw=sum(1 for r in rows if r.get('parse_status')=='raw_preserved')
    now=datetime.now(timezone.utc).isoformat()
    log={'database_id':item['id'],'database_name':item['name'],'district_name':district.strip(),'upazila_name':upazila.strip(),
         'file_name':file.filename,'records_detected':len(rows),'records_written':written,'batch_commits':batches,
         'records_added_or_updated':written,'raw_preserved':raw,'created_at':now,'uploaded_by':user.get('email',''),
         'parser':'PY-RENDER-V9-MULTI-TURSO'}
    conn=connect_item(item)
    try:
        iid='import_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
        conn.execute('INSERT INTO pdf_imports(id,database_id,district_name,upazila_name,file_name,records_detected,records_written,created_at,uploaded_by,parser) VALUES (?,?,?,?,?,?,?,?,?,?)',
                     (iid,item['id'],district.strip(),upazila.strip(),file.filename,len(rows),written,now,user.get('email',''),log['parser']))
        conn.commit()
    finally: conn.close()
    return {'ok':True,**log}

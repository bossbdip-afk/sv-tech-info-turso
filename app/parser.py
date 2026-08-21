import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import fitz  # PyMuPDF

BN_DIGITS = '০১২৩৪৫৬৭৮৯'
EN_DIGITS = '0123456789'


def clean(s: str) -> str:
    return re.sub(r'\s+', ' ', str(s or '').replace('\u00a0', ' ')).strip()


def bn_to_en(s: str) -> str:
    return str(s or '').translate(str.maketrans(BN_DIGITS, EN_DIGITS))

LEGACY_RE = re.compile(
    r'[\x80-\x9FËÎÏÐÑÒÔ×ØÙÚÌåêîïõøúûýÿĀăĐēĔėęĢĤĥĦħĨĩĮįıĲĳĴĽĺļńŇŌŐŘřŜśŝšŞŢŦũŬŮűŽžſƀƁƂƃƄƅƆƎƏƣŨūŋ¢µàŀ◌]'
)
GLYPH_MAP = {
    '\x83': 'ট্', '\x8c': 'ন্', '\x98': 'হ্',
    'Ë': '্য', 'Î': '\uE001', 'Ï': 'ে', 'Ð': 'ৈ', 'Ñ': 'ক', 'Ò': 'ক্ট', 'Ô': 'ক্স', '×': 'ক্ত', 'Ù': 'ক্ষ',
    'ê': 'ঙ্গ', 'î': 'চ্চ', 'ï': 'চ্ছ', 'õ': 'জ্জ', 'ú': 'জ্ব', 'û': 'ঞ্চ', 'ý': 'ঞ্জ', 'ÿ': 'ট্ট',
    'Ā': 'ট্র', 'ă': 'ড্র', 'Đ': 'ত্ত', 'ē': 'ত্ত', 'Ĕ': 'ত্র', 'ė': 'দ্দ', 'ę': 'দ্র',
    'Ģ': 'ন্ত', 'Ĥ': 'ন্দ', 'ĥ': 'ন্ম', 'Ħ': 'ন্ধ', 'ħ': 'ন্দ্র', 'Ĩ': 'ন্ন', 'ĩ': 'ন্স',
    'į': 'ন্য', 'ı': 'প্ল', 'ĳ': '্বপ্ন', 'Ĵ': 'প্র', 'İ': 'প্ত', 'Ľ': 'ব্র', 'ĺ': 'ব্দ', 'ļ': 'ব্ব', 'ł': 'ম্প',
    'ń': 'ম্ব', 'Ň': 'ম্ম', 'Ō': 'ল্ল', 'Ő': 'উল', 'Ř': 'শ্র', 'ř': 'শ্য', 'ś': 'শ্ব', 'ŝ': 'ষ্ণ', 'š': 'ষ্প',
    'Ş': 'স্ট', 'Ţ': 'স্ট্র', 'ũ': 'স্ত', 'Ů': 'স্ব', 'ű': 'স্ত্র', 'Ž': 'গু', 'ž': 'ম্ভু', 'ſ': 'নু', 'ƀ': 'সু',
    'Ɓ': 'রু', 'Ƃ': 'রূ', 'ƃ': 'দু', 'Ƅ': 'শু', 'ƅ': 'হৃ', 'Ɔ': 'হু', 'Ǝ': 'স্ত', 'Ə': 'নু', 'ƣ': 'কু',
    'Ũ': 'স্ট', 'Ú': 'ক্ষ্ম', 'Ì': '্ল', 'Ĳ': 'ড়', 'Ŭ': 'ষ্প', 'Ŧ': 'স্ক', 'Į': 'স্ত্র', 'ŀ': 'ভ', '¢': 'ন', 'å': 'গ্রে',
    'ŋ': 'ল',
}
SAFE_CONTEXT_REPLACEMENTS = [
    (re.compile(r'øানেন্দ্র|øােনন্দ্র'), 'জ্ঞানেন্দ্র'),
    (re.compile(r'মা\s*Ũ[^\u0980-\u09FF]{0,4}র'), 'মাস্টার'),
    (re.compile(r'জো\s*ū\s*া'), 'জ্যোৎস্না'),
    (re.compile(r'ি\s*ū\s*à\s*া'), 'রিশা'),
    (re.compile(r'ø\s*া\s*েন\s*্দ্র'), 'জ্ঞানেন্দ্র'),
    (re.compile(r'ø[^\u0980-\u09FF]{0,4}েন্দ্র'), 'জ্ঞানেন্দ্র'),
    (re.compile(r'আ\s*Ø\s*াম'), 'আজম'),
    (re.compile(r'কৃ\s*\x96\s*ঞ'), 'কৃষ্ণ'),
    (re.compile(r'ব\s*\x87\s*দ্য'), 'বৈদ্য'),
    (re.compile(r'বাি[µμ]'), 'বাড়ি'),
    (re.compile(r'দিŜণ'), 'দক্ষিণ'),
    (re.compile(r'\u008Aক্ষিণ'), 'দক্ষিণ'),
    (re.compile(r'শŅ\s+বাসফর'), 'শ্যাম বাসফর'),
    (re.compile(r'তাবা\u0097সুম'), 'তাবাসসুম'),
    (re.compile(r'পাéী\s+রাজভর'), 'পাখী রাজভর'),
    (re.compile(r'তĪী'), 'তিথী'),
]

def legacy_source(s: str) -> bool:
    x = str(s or '')
    if LEGACY_RE.search(x):
        return True
    return bool(re.search(r'(?:^|[\s:,(])(?:ি|ে|ৈ)[ক-হড়ঢ়য়]|[ক-হড়ঢ়য়]া[িেৈ][ক-হড়ঢ়য়]|তািরখ|ি[পঠ]তা|িঠকানা|ইউিনয়ন|উপেজলা|ভাটার', x))

def clean_unicode_bangla(s: str) -> str:
    x = unicodedata.normalize('NFC', str(s or '')).replace('\u00a0', ' ')
    x = re.sub(r'[\u0000\u200b-\u200f\ufeff\u25cc]', '', x)
    for pat, repl in SAFE_CONTEXT_REPLACEMENTS:
        x = pat.sub(repl, x)
    for ch, repl in {'ŋ': 'ল', 'ł': 'ম্প', 'İ': 'প্ত', 'Ľ': 'ব্র', 'ŝ': 'ষ্ণ', 'š': 'ষ্প', 'Ũ': 'স্ট', 'Ú': 'ক্ষ্ম', 'Ì': '্ল', 'Ǝ': 'স্ত', 'Ƃ': 'রূ', 'Ĳ': 'ড়', 'Ŭ': 'ষ্প', 'Ŧ': 'স্ক', 'Į': 'স্ত্র', 'ŀ': 'ভ', '¢': 'ন', 'å': 'গ্রে'}.items():
        x = x.replace(ch, repl)
    x = re.sub(r'\s*,\s*,+', ', ', x)
    x = re.sub(r'(^|\s)[,;]+\s*', r'\1', x)
    x = re.sub(r'\s+([।,:;])', r'\1', x)
    return clean(x)

def _reorder_prebase(x: str) -> str:
    consonant = re.compile(r'[ক-হড়ঢ়য়]')
    out = []
    i = 0
    while i < len(x):
        mark = x[i]
        if mark in ('ি', 'ে', 'ৈ') and i + 1 < len(x) and consonant.match(x[i + 1]):
            j = i + 1
            cluster = x[j]
            j += 1
            while j + 1 < len(x) and x[j] == '্' and consonant.match(x[j + 1]):
                cluster += x[j:j + 2]
                j += 2
            if mark == 'ে' and j < len(x) and x[j] == 'া':
                out.append(cluster + 'ো')
                i = j + 1
                continue
            if mark == 'ে' and j < len(x) and x[j] == 'ৗ':
                out.append(cluster + 'ৌ')
                i = j + 1
                continue
            out.append(cluster + mark)
            i = j
            continue
        out.append(mark)
        i += 1
    return ''.join(out)

def repair_legacy_bangla(s: str) -> str:
    x = unicodedata.normalize('NFC', str(s or '')).replace('\u00a0', ' ')
    x = re.sub(r'[\u0000\u200b-\u200f\ufeff\u25cc]', '', x)
    if not legacy_source(x):
        return clean_unicode_bangla(x)
    x = ''.join(GLYPH_MAP.get(ch, ch) for ch in x)
    for _ in range(3):
        x = re.sub(r'([ক-হড়ঢ়য়])\uE001', r'র্\1', x)
    x = x.replace('\uE001', 'র্')
    x = _reorder_prebase(x)
    replacements = {
        'উপেজলা': 'উপজেলা', 'ইউিনয়ন': 'ইউনিয়ন', 'ইউিনয়ন': 'ইউনিয়ন', 'ইউনিয়ন': 'ইউনিয়ন',
        'ভাটার': 'ভোটার', 'তািরখ': 'তারিখ', 'িপতা': 'পিতা', 'িঠকানা': 'ঠিকানা', 'পিরষেদর': 'পরিষদের',
        'ওয়াড': 'ওয়ার্ড', 'ওয়াড': 'ওয়ার্ড', 'পোস্টেকোড': 'পোস্ট কোড', 'পোস্টকোড': 'পোস্ট কোড',
    }
    for a, b in replacements.items():
        x = x.replace(a, b)
    x = re.sub(r'জন্ম\s*তারখ', 'জন্ম তারিখ', x)
    x = re.sub(r'জন্ম\s*তািরখ', 'জন্ম তারিখ', x)
    for pat, repl in SAFE_CONTEXT_REPLACEMENTS:
        x = pat.sub(repl, x)
    return clean_unicode_bangla(x)

def repair_bangla(s: str) -> str:
    return repair_legacy_bangla(s) if legacy_source(s) else clean_unicode_bangla(s)


def clean_field(s: str) -> str:
    x = clean_unicode_bangla(s)
    # Some legacy Bangla PDFs extract chandrabindu before the aa-kar (e.g. 'চঁান').
    # Standard Unicode order is aa-kar + chandrabindu ('চাঁন'). Apply this to every
    # field so names are fixed too, not only addresses.
    x = x.replace('ঁা', 'াঁ')
    x = re.sub(r"([ক-হড়ঢ়য়ািীুূৃেৈোৌংঃঁ])['’`]([ক-হড়ঢ়য়ািীুূৃেৈোৌংঃঁ])", r'\1\2', x)
    x = re.sub(r'^[\s,.;:।]+', '', x)
    x = re.sub(r'[\s,;]+$', '', x)
    return x.strip()

def normalize_profession(s: str) -> str:
    x = clean_field(s)
    if re.fullmatch(r'(?:গৃহিনী|গৃহীনি|গিহিনী)', x):
        return 'গৃহিণী'
    return x


@dataclass
class Item:
    text: str
    x: float
    y: float
    width: float


@dataclass
class Row:
    y: float
    items: List[Item]
    text: str

def line_text(items: List[Item]) -> str:
    parts = sorted(items, key=lambda z: z.x)
    out = ''
    prev = None
    for it in parts:
        if prev:
            gap = it.x - (prev.x + prev.width)
            if gap > 2.2 and not out.endswith(' ') and not it.text.startswith(' '):
                out += ' '
        out += it.text
        prev = it
    return clean(out)

def get_rows(page: fitz.Page) -> List[Row]:
    data = page.get_text('dict', sort=False)
    items: List[Item] = []
    for block in data.get('blocks', []):
        for line in block.get('lines', []):
            for span in line.get('spans', []):
                text = str(span.get('text', ''))
                if not text:
                    continue
                x0, y0, x1, y1 = span.get('bbox', (0, 0, 0, 0))
                items.append(Item(text=text, x=float(x0), y=(float(y0)+float(y1))/2, width=float(x1)-float(x0)))
    items.sort(key=lambda z: (z.y, z.x))
    rows: List[Tuple[float, List[Item]]] = []
    for item in items:
        found = None
        for idx, (ry, ritems) in enumerate(rows):
            if abs(ry - item.y) < 2.5:
                found = idx
                break
        if found is None:
            rows.append((item.y, [item]))
        else:
            ry, ritems = rows[found]
            count = len(ritems)
            ritems.append(item)
            rows[found] = ((ry * count + item.y) / (count + 1), ritems)
    rows.sort(key=lambda t: t[0])
    result = []
    for ry, ritems in rows:
        txt = line_text(ritems)
        if txt:
            result.append(Row(y=ry, items=sorted(ritems, key=lambda z: z.x), text=txt))
    return result

def serial_digits(s: str) -> str:
    return ''.join(re.findall(r'[0-9০-৯]', str(s or '')))

def detect_anchors(rows: List[Row]):
    anchors = []
    for row in rows:
        parts = sorted(row.items, key=lambda z: z.x)
        for it in parts:
            raw = it.text
            fixed = repair_bangla(raw)
            m = re.search(r'(?:^|\s)([0-9০-৯]{1,6})\s*[.।)]\s*', raw) or re.search(r'(?:^|\s)([0-9০-৯]{1,6})\s*[.।)]\s*', fixed)
            if not m:
                m = re.match(r'^\s*([0-9০-৯]{1,6})\s*$', raw) or re.match(r'^\s*([0-9০-৯]{1,6})\s*$', fixed)
            if not m:
                continue
            near = ' '.join(z.text for z in parts if z.x >= it.x - 3 and z.x <= it.x + 145)
            if re.search(r'নাম\s*:', repair_bangla(raw)) or re.search(r'নাম\s*:', repair_bangla(near)):
                serial = serial_digits(m.group(1))
                if serial and not any(a['serial'] == serial and abs(a['y'] - row.y) < 2.8 and abs(a['x'] - it.x) < 8 for a in anchors):
                    anchors.append({'serial': serial, 'x': it.x, 'y': row.y})
    return anchors

def extract_meta(rows: List[Row], anchors, prev: Dict[str, str]):
    if not anchors:
        return dict(prev)
    first_y = min(a['y'] for a in anchors)
    header_rows = [r for r in rows if r.y < first_y - 2]
    raw_header = ' '.join(r.text for r in header_rows)
    header = repair_bangla(raw_header)
    meta = dict(prev)

    def pick(pattern, src=header):
        m = re.search(pattern, src, re.I)
        return clean(m.group(1)) if m else ''
    raw_ward = re.search(r'ওয়াড\s*Î?\s*(?:নং|ন(?:ń|ং)?র)\s*(?:[-–—:]|：)?\s*([0-9০-৯]{1,2})', raw_header, re.I)
    raw_post = re.search(r'Ï?পা.{0,4}েকাড\s*[:：]\s*([0-9০-৯]{4})', raw_header, re.I)
    meta['district_name'] = pick(r'জেলা\s*[:：]\s*(.*?)\s+(?=উপজেলা|ইউনিয়ন|ডাকঘর|ভোটার)') or meta.get('district_name', '')
    meta['upazila_name'] = pick(r'উপজেলা(?:/থানা)?\s*[:：]\s*(.*?)\s+(?=ইউনিয়ন|ডাকঘর|ভোটার)') or meta.get('upazila_name', '')
    meta['union_name'] = pick(r'ইউনিয়ন(?:\s*/\s*ওয়ার্ড\s*/\s*ক্যাঃ?\s*বোঃ?)?\s*[:：]\s*([^\s]+)') or meta.get('union_name', '')
    meta['post_office'] = pick(r'ডাকঘর\s*[:：]\s*([^\s]+)') or meta.get('post_office', '')
    meta['post_code'] = (clean(raw_post.group(1)) if raw_post else '') or pick(r'পোস্ট\s*কোড\s*[:：]\s*([0-9০-৯]+)') or meta.get('post_code', '')
    meta['voter_area_code'] = pick(r'ভোটার\s*এলাকার\s*(?:নং|নম্বর|কোড)\s*[:：]\s*([0-9০-৯]+)') or meta.get('voter_area_code', '')
    meta['voter_area'] = pick(r'ভোটার\s*এলাকার\s*নাম\s*[:：]\s*(.*?)(?=\s+ভোটার\s*এলাকার|$)') or meta.get('voter_area', '')
    meta['ward_no'] = (clean(raw_ward.group(1)) if raw_ward else '') or pick(r'ওয়ার্ড\s*(?:নম্বর|নং)\s*(?:\([^)]*\))?\s*(?:[-–—:]|：)?\s*([0-9০-৯]+)') or meta.get('ward_no', '')
    return meta


def clean_address(value: str, page_no: int, district: str) -> str:
    """Return the voter record's own address, cleaned only of PDF layout artifacts.

    Important: do NOT merge page-header metadata (post office / post code / ward)
    into the address. Those values already have dedicated Firestore fields.
    """
    x = clean_field(value)

    # Legacy Bijoy-style extraction can place chandrabindu before the aa-kar,
    # e.g. "গঁাও". Reorder it to standard Unicode display: "গাঁও".
    x = x.replace('\u0981\u09be', '\u09be\u0981')

    # Page/header text can bleed into the last record of a column. Remove only
    # known layout labels while preserving address text that may continue after
    # the label (e.g. "... ইসলামপুর, ন্ত ভোটার তালিকাজামালপুর").
    x = re.sub(r'\s*(?:চূড়ান্ত|চূড়ান্ত|ন্ত)?\s*ভোটার\s*তালিকা\s*', ' ', x)
    x = re.sub(r'\s*রেজিস্ট্রেশন\s*অফিসার\s*$', '', x)

    # Remove a leaked PDF page number only when it equals the current page.
    m = re.search(r'\s+([0-9০-৯]{1,3})\s*$', x)
    if m and bn_to_en(m.group(1)) == str(page_no):
        x = x[:m.start()].strip()

    # Normalize separators introduced when a header label was removed.
    x = re.sub(r'\s*,\s*', ', ', x)
    x = re.sub(r',\s*,+', ', ', x)
    x = re.sub(r'\s{2,}', ' ', x)
    return clean_field(x)


def _field(text: str, pattern: str) -> str:
    m = re.search(pattern, text)
    return clean_field(m.group(1)) if m else ''


def _suspicious(v: str) -> bool:
    s = str(v or '')
    return bool(re.search(r'[\x80-\x9FĥĔėƣŘƃƁĤſŌƀËõøĢĺĴÐŇĨ×ÎÙêēęĽłăŞűįĦƆƂŽÑũûîŮýħåıéūůżŅ]', s) or re.search(r'তািরখ|উপেজলা|ইউিনয়ন|ভাটার|ি[পঠজ]তা|িঠকানা|Ï|Ð', s))

def parse_page(page: fitz.Page, district: str, upazila: str, file_name: str, carry_meta: Dict[str, str], page_no: int):
    rows = get_rows(page)
    anchors = detect_anchors(rows)
    if not anchors:
        return [], carry_meta
    meta = extract_meta(rows, anchors, {**carry_meta, 'district_name': district, 'upazila_name': upazila})
    xs = sorted(set(round(a['x'], 1) for a in anchors))
    centers = []
    for x in xs:
        if not centers or abs(x - centers[-1]) > 20:
            centers.append(x)
    width = float(page.rect.width)
    gaps = [centers[i] - centers[i-1] for i in range(1, len(centers))]
    col_gap = min(gaps) if gaps else width / max(1, len(centers))
    left_pad = min(10, col_gap * 0.08)
    bounds = []
    for i, c in enumerate(centers):
        right = centers[i+1] - left_pad if i < len(centers)-1 else min(width, c + col_gap - left_pad)
        bounds.append((max(0, c-left_pad), right))
    by_col = []
    for c in centers:
        by_col.append(sorted([a for a in anchors if abs(a['x'] - c) <= 20], key=lambda a: a['y']))
    out = []
    for a in anchors:
        ci = min(range(len(centers)), key=lambda i: abs(centers[i] - a['x']))
        col_anchors = by_col[ci]
        idx = col_anchors.index(a)
        next_a = col_anchors[idx+1] if idx + 1 < len(col_anchors) else None
        top = a['y'] - 2
        bottom = (next_a['y'] - 3) if next_a else float(page.rect.height)
        left, right = bounds[ci]
        cell_lines = []
        for row in rows:
            if row.y < top or row.y >= bottom:
                continue
            cell_items = [it for it in row.items if it.x >= left and it.x < right]
            if cell_items:
                cell_lines.append(line_text(cell_items))
        raw_cell = ' '.join(cell_lines)
        ctext = repair_bangla(raw_cell)
        serial = repair_bangla(a['serial'])
        name = _field(ctext, r'নাম\s*:\s*(.*?)(?=\s*ভোটার\s*নং\s*:|$)')
        voter = _field(ctext, r'ভোটার\s*নং\s*:\s*([0-9০-৯]+)')
        if not name:
            continue
        raw_address_value = _field(ctext, r'ঠিকানা\s*:\s*(.*?)(?=\s+[0-9০-৯]{1,6}\s*[.।)]\s*নাম\s*:|$)')
        row = {
            'district_name': district or '', 'upazila_name': upazila or '', 'serial_no': serial,
            'name': name, 'voter_no': voter,
            'father_name': _field(ctext, r'পিতা\s*:\s*(.*?)(?=\s*মাতা\s*:|$)'),
            'mother_name': _field(ctext, r'মাতা\s*:\s*(.*?)(?=\s*পেশা\s*:|$)'),
            'profession': normalize_profession(_field(ctext, r'পেশা\s*:\s*(.*?)(?=\s*জন্ম\s*তারিখ\s*:|$)')),
            'birth_date': _field(ctext, r'জন্ম\s*তারিখ\s*:\s*([0-9০-৯/.-]+)'),
            'address': clean_address(raw_address_value, page_no, district),
            'union_name': meta.get('union_name', ''), 'post_office': meta.get('post_office', ''), 'post_code': meta.get('post_code', ''),
            'voter_area': meta.get('voter_area', ''), 'voter_area_code': meta.get('voter_area_code', ''), 'ward_no': meta.get('ward_no', ''),
            'source_file': file_name, 'created_at': datetime.now(timezone.utc).isoformat(),
            'parser_version': 'PY-RENDER-V5-FATHER-UNICODE-FIXED', 'text_encoding': 'unicode-bn-server-v1',
            'raw_pdf_text': raw_cell, 'parser_source_text': ctext,
        }
        row['raw_name'] = row['name']
        row['raw_father_name'] = row['father_name']
        row['raw_mother_name'] = row['mother_name']
        row['raw_profession'] = row['profession']
        row['raw_address'] = raw_address_value
        warnings = []
        for k in ('name','father_name','mother_name','profession','address'):
            if _suspicious(row[k]):
                warnings.append(k)
        row['parse_status'] = 'raw_preserved' if warnings else 'clean'
        row['parser_warning_text'] = ','.join(warnings)
        row['record_key'] = f"v:{voter}" if voter else f"s:{district}|{upazila}|{file_name}|{serial}"
        out.append(row)
    return out, meta

def parse_pdf_bytes(data: bytes, district: str, upazila: str, file_name: str) -> List[Dict[str, str]]:
    doc = fitz.open(stream=data, filetype='pdf')
    out: List[Dict[str, str]] = []
    carry = {'district_name': district, 'upazila_name': upazila, 'union_name': '', 'post_office': '', 'post_code': '', 'voter_area': '', 'voter_area_code': '', 'ward_no': ''}
    try:
        for i in range(doc.page_count):
            rows, meta = parse_page(doc.load_page(i), district, upazila, file_name, carry, i + 1)
            out.extend(rows)
            for k, v in meta.items():
                if str(v or '').strip():
                    carry[k] = v
    finally:
        doc.close()
    seen = set()
    unique = []
    for r in out:
        key = (r.get('serial_no',''), r.get('voter_no',''))
        if key in seen:
            continue
        seen.add(key)
        unique.append(r)
    return unique

from flask import Flask, request, jsonify, send_file, render_template, session, redirect, url_for
import openpyxl
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter
import io, os, sqlite3, hashlib, secrets, datetime
from collections import defaultdict
from functools import wraps

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))

DB_PATH = os.environ.get('DB_PATH', 'gst_portal.db')

# ══════════════════════════════════════════════════════════
# DATABASE SETUP
# ══════════════════════════════════════════════════════════
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()

    # Users table
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        full_name TEXT,
        role TEXT DEFAULT 'user',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )''')

    # Hotels table
    c.execute('''CREATE TABLE IF NOT EXISTS hotels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        code TEXT UNIQUE NOT NULL,
        gstin TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )''')

    # User-Hotel mapping
    c.execute('''CREATE TABLE IF NOT EXISTS user_hotels (
        user_id INTEGER,
        hotel_id INTEGER,
        PRIMARY KEY (user_id, hotel_id),
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (hotel_id) REFERENCES hotels(id)
    )''')

    # Processing history
    c.execute('''CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        hotel_id INTEGER,
        module TEXT NOT NULL,
        step TEXT NOT NULL,
        filename TEXT NOT NULL,
        status TEXT DEFAULT 'success',
        processed_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')

    # Saved files — hotel-wise persistent file storage
    c.execute('''CREATE TABLE IF NOT EXISTS saved_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hotel_id INTEGER NOT NULL,
        file_type TEXT NOT NULL,
        filename TEXT NOT NULL,
        file_data BLOB NOT NULL,
        file_size INTEGER,
        uploaded_by INTEGER,
        uploaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(hotel_id, file_type),
        FOREIGN KEY (hotel_id) REFERENCES hotels(id),
        FOREIGN KEY (uploaded_by) REFERENCES users(id)
    )''')

    # Create default admin if not exists
    admin_exists = c.execute("SELECT id FROM users WHERE username='admin'").fetchone()
    if not admin_exists:
        pw_hash = hashlib.sha256('admin123'.encode()).hexdigest()
        c.execute("INSERT INTO users (username, password_hash, full_name, role) VALUES (?,?,?,?)",
                  ('admin', pw_hash, 'Administrator', 'admin'))
        # Default hotels
        c.execute("INSERT OR IGNORE INTO hotels (name, code, gstin) VALUES (?,?,?)",
                  ('TWG Hotels - Jaipur Marriott', 'JMH', 'XXXXXXXXXXXXXXXXX'))
        c.execute("INSERT OR IGNORE INTO hotels (name, code, gstin) VALUES (?,?,?)",
                  ('TWG Hotels - Chennai', 'CHN', 'XXXXXXXXXXXXXXXXX'))
        conn.commit()

    conn.commit()
    conn.close()

def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

# ── Auth decorator ─────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            if request.is_json or request.method == 'POST':
                return jsonify({'error': 'Login required', 'redirect': '/login'}), 401
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated

def save_history(module, step, filename, hotel_id=None, status='success'):
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO history (user_id, hotel_id, module, step, filename, status) VALUES (?,?,?,?,?,?)",
            (session.get('user_id'), hotel_id, module, step, filename, status)
        )
        conn.commit()
        conn.close()
    except:
        pass

# ══════════════════════════════════════════════════════════
# EXCEL HELPERS
# ══════════════════════════════════════════════════════════
FMT_COMMA = '_ * #,##0.00_ ;_ * \\-#,##0.00_ ;_ * \\-??_ ;_ @_ '
FMT_INT   = '#,##0'
FMT_PCT   = '0%'

def hdr_font():  return Font(name='Calibri', size=10, bold=True, color='FFFFFFFF')
def hdr_fill():  return PatternFill('solid', fgColor='FF000000')
def data_font(): return Font(name='Calibri', size=10)
def bold_font(): return Font(name='Calibri', size=10, bold=True)

def apply_header(ws, row, start_col, end_col):
    for c in range(start_col, end_col + 1):
        ws.cell(row, c).font = hdr_font()
        ws.cell(row, c).fill = hdr_fill()

def apply_total_row(ws, row, start_col, end_col):
    for c in range(start_col, end_col + 1):
        ws.cell(row, c).font = bold_font()
        ws.cell(row, c).fill = hdr_fill()

def set_heights(ws, last_row):
    for r in range(1, last_row + 2):
        ws.row_dimensions[r].height = 13

def read_wb(file_bytes):
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
    out = {}
    for name in wb.sheetnames:
        out[name] = [list(r) for r in wb[name].iter_rows(values_only=True)]
    return out

def get_sheet(data, *names):
    for n in names:
        if n in data:
            return data[n]
    return list(data.values())[0]

def send_wb(wb, filename):
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return send_file(buf, download_name=filename,
                     as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


# ══════════════════════════════════════════════════════════
# SAVED FILES API
# ══════════════════════════════════════════════════════════

FILE_TYPES = {
    'backend':  'Backend Trx, SAC Mapping',
    'd110':     'D110 Dr. Base',
    'd140':     'D140 Dr (Journal)',
    'gl':       'GL File (Peoplesoft)',
    'tb':       'Trial Balance (TB)',
    'einv':     'GSTR-4A E-Invoice',
}

def save_file_to_db(hotel_id, file_type, filename, file_bytes):
    conn = get_db()
    conn.execute("""
        INSERT OR REPLACE INTO saved_files
        (hotel_id, file_type, filename, file_data, file_size, uploaded_by)
        VALUES (?,?,?,?,?,?)
    """, (hotel_id, file_type, filename, file_bytes, len(file_bytes), session.get('user_id')))
    conn.commit()
    conn.close()

def load_file_from_db(hotel_id, file_type):
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM saved_files WHERE hotel_id=? AND file_type=?",
        (hotel_id, file_type)
    ).fetchone()
    conn.close()
    return dict(row) if row else None

def get_file_bytes(hotel_id, file_type, request_file_key):
    """Get file bytes — from request if uploaded, else from DB"""
    f = request.files.get(request_file_key)
    if f and f.filename:
        return f.read()
    # Try saved DB
    saved = load_file_from_db(hotel_id, file_type)
    if saved:
        return bytes(saved['file_data'])
    return None

@app.route('/api/files/<int:hotel_id>', methods=['GET'])
@login_required
def list_saved_files(hotel_id):
    conn = get_db()
    rows = conn.execute("""
        SELECT sf.id, sf.file_type, sf.filename, sf.file_size, sf.uploaded_at,
               u.full_name as uploaded_by_name
        FROM saved_files sf
        LEFT JOIN users u ON sf.uploaded_by = u.id
        WHERE sf.hotel_id = ?
        ORDER BY sf.uploaded_at DESC
    """, (hotel_id,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/files/upload', methods=['POST'])
@login_required
def upload_saved_file():
    hotel_id  = request.form.get('hotel_id')
    file_type = request.form.get('file_type')
    f = request.files.get('file')

    if not hotel_id or not file_type or not f:
        return jsonify({'error': 'hotel_id, file_type and file required'}), 400
    if file_type not in FILE_TYPES:
        return jsonify({'error': 'Invalid file type'}), 400

    file_bytes = f.read()
    save_file_to_db(int(hotel_id), file_type, f.filename, file_bytes)
    return jsonify({
        'success': True,
        'message': f'{FILE_TYPES[file_type]} saved for hotel',
        'filename': f.filename,
        'size': len(file_bytes)
    })

@app.route('/api/files/delete/<int:hotel_id>/<file_type>', methods=['DELETE'])
@login_required
def delete_saved_file(hotel_id, file_type):
    conn = get_db()
    conn.execute("DELETE FROM saved_files WHERE hotel_id=? AND file_type=?",
                 (hotel_id, file_type))
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'message': f'{FILE_TYPES.get(file_type,file_type)} deleted'})

# ══════════════════════════════════════════════════════════
# AUTH ROUTES
# ══════════════════════════════════════════════════════════
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        if 'user_id' in session:
            return redirect('/')
        return render_template('login.html')

    data = request.get_json() or request.form
    username = str(data.get('username', '')).strip()
    password = str(data.get('password', '')).strip()

    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE username=? AND password_hash=?",
        (username, hash_pw(password))
    ).fetchone()
    conn.close()

    if not user:
        return jsonify({'error': 'Invalid username or password'}), 401

    session['user_id']   = user['id']
    session['username']  = user['username']
    session['full_name'] = user['full_name']
    session['role']      = user['role']
    return jsonify({'success': True, 'redirect': '/'})

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')

@app.route('/api/me')
@login_required
def me():
    conn = get_db()
    hotels = conn.execute('''
        SELECT h.* FROM hotels h
        JOIN user_hotels uh ON h.id = uh.hotel_id
        WHERE uh.user_id = ?
        UNION
        SELECT h.* FROM hotels h WHERE ? = (SELECT id FROM users WHERE role='admin' AND id=?)
    ''', (session['user_id'], session['user_id'], session['user_id'])).fetchall()

    if session.get('role') == 'admin':
        hotels = conn.execute("SELECT * FROM hotels").fetchall()

    conn.close()
    return jsonify({
        'user_id':   session['user_id'],
        'username':  session['username'],
        'full_name': session['full_name'],
        'role':      session['role'],
        'hotels':    [dict(h) for h in hotels]
    })

# ══════════════════════════════════════════════════════════
# ADMIN ROUTES
# ══════════════════════════════════════════════════════════
def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('role') != 'admin':
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated

@app.route('/api/admin/users', methods=['GET'])
@login_required
@admin_required
def get_users():
    conn = get_db()
    users = conn.execute("SELECT id, username, full_name, role, created_at FROM users").fetchall()
    conn.close()
    return jsonify([dict(u) for u in users])

@app.route('/api/admin/users', methods=['POST'])
@login_required
@admin_required
def create_user():
    data = request.get_json()
    username  = data.get('username', '').strip()
    password  = data.get('password', '').strip()
    full_name = data.get('full_name', '').strip()
    role      = data.get('role', 'user')
    hotel_ids = data.get('hotel_ids', [])

    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400

    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO users (username, password_hash, full_name, role) VALUES (?,?,?,?)",
            (username, hash_pw(password), full_name, role)
        )
        user_id = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()['id']
        for hid in hotel_ids:
            conn.execute("INSERT OR IGNORE INTO user_hotels VALUES (?,?)", (user_id, hid))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': f'User {username} created'})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'Username already exists'}), 400

@app.route('/api/admin/users/<int:uid>/password', methods=['POST'])
@login_required
@admin_required
def change_password(uid):
    data = request.get_json()
    new_pw = data.get('password', '').strip()
    if not new_pw:
        return jsonify({'error': 'Password required'}), 400
    conn = get_db()
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_pw(new_pw), uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True})

@app.route('/api/admin/users/<int:uid>', methods=['DELETE'])
@login_required
@admin_required
def delete_user(uid):
    conn = get_db()
    conn.execute("DELETE FROM users WHERE id=?", (uid,))
    conn.execute("DELETE FROM user_hotels WHERE user_id=?", (uid,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})

@app.route('/api/admin/hotels', methods=['GET'])
@login_required
@admin_required
def get_hotels():
    conn = get_db()
    hotels = conn.execute("SELECT * FROM hotels").fetchall()
    conn.close()
    return jsonify([dict(h) for h in hotels])

@app.route('/api/admin/hotels', methods=['POST'])
@login_required
@admin_required
def create_hotel():
    data = request.get_json()
    name  = data.get('name', '').strip()
    code  = data.get('code', '').strip().upper()
    gstin = data.get('gstin', '').strip()
    if not name or not code:
        return jsonify({'error': 'Name and code required'}), 400
    try:
        conn = get_db()
        conn.execute("INSERT INTO hotels (name, code, gstin) VALUES (?,?,?)", (name, code, gstin))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'Hotel code already exists'}), 400

@app.route('/api/admin/hotels/<int:hid>', methods=['DELETE'])
@login_required
@admin_required
def delete_hotel(hid):
    conn = get_db()
    conn.execute("DELETE FROM hotels WHERE id=?", (hid,))
    conn.execute("DELETE FROM user_hotels WHERE hotel_id=?", (hid,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})

# ══════════════════════════════════════════════════════════
# HISTORY ROUTES
# ══════════════════════════════════════════════════════════
@app.route('/api/history')
@login_required
def get_history():
    conn = get_db()
    if session.get('role') == 'admin':
        rows = conn.execute('''
            SELECT h.*, u.username, u.full_name, ht.name as hotel_name
            FROM history h
            JOIN users u ON h.user_id = u.id
            LEFT JOIN hotels ht ON h.hotel_id = ht.id
            ORDER BY h.processed_at DESC LIMIT 100
        ''').fetchall()
    else:
        rows = conn.execute('''
            SELECT h.*, u.username, u.full_name, ht.name as hotel_name
            FROM history h
            JOIN users u ON h.user_id = u.id
            LEFT JOIN hotels ht ON h.hotel_id = ht.id
            WHERE h.user_id = ?
            ORDER BY h.processed_at DESC LIMIT 50
        ''', (session['user_id'],)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

# ══════════════════════════════════════════════════════════
# MAIN ROUTES
# ══════════════════════════════════════════════════════════
@app.route('/')
@login_required
def index():
    return render_template('index.html')

@app.route('/admin')
@login_required
@admin_required
def admin_panel():
    return render_template('admin.html')

# ══════════════════════════════════════════════════════════
# BUILD MAPPING
# ══════════════════════════════════════════════════════════
def build_map(backend_rows):
    m = {}
    for row in backend_rows[2:]:
        if not row or not row[1]:
            continue
        key = str(row[1]).strip().lower()
        m[key] = {
            'tax_head': row[2],
            'rate':     row[3],
            'hsn_desc': row[4],
            'hsn_code': row[5],
            'bof_code': row[6],
        }
    return m

def build_einv_map(gstr_rows):
    einv = {}
    for row in gstr_rows[1:]:
        if not row or not row[2]: continue
        bill = str(row[2]).strip()
        einv[bill] = {
            'gstin':   str(row[0] or '').strip(),
            'name':    str(row[1] or '').strip(),
            'inv_type':str(row[8] or '').strip(),
            'taxable': float(row[11] or 0) if row[11] else 0,
        }
    return einv

# ══════════════════════════════════════════════════════════
# D110 PROCESSING (all existing functions preserved)
# ══════════════════════════════════════════════════════════
def process_d110(d110_rows, mapping, einv_map):
    KEEP = 14
    new_hdrs = list(d110_rows[0])[:KEEP] + [
        'Tax Head','Rate','HSN Description','HSN Code',
        'Rate Check','Invoice Check','GSTIN','Receiver Name',
        'Invoice Type','Document Type','Supply Type'
    ]
    processed = []
    for row in d110_rows[1:]:
        if not any(row): continue
        base = list(row)[:KEEP]
        while len(base) < KEEP: base.append(None)

        trx_desc = str(base[7] or '').strip().lower()
        m = mapping.get(trx_desc, {})
        tax_head = m.get('tax_head','')
        rate     = m.get('rate','')
        hsn_desc = m.get('hsn_desc','')
        hsn_code = m.get('hsn_code','')

        try:    debit = float(base[10] or 0)
        except: debit = 0

        rate_str = str(rate).strip().upper() if rate is not None else ''
        if rate_str in ('NO GST','NON GST'):
            rate_check = 0
        elif rate_str in ('','0','NONE') or rate is None or rate == 0:
            rate_check = debit * -1
        else:
            try:
                rn = float(rate)
                rate_check = debit / rn if rn != 0 else debit * -1
            except:
                rate_check = debit * -1

        bill_no = str(base[2] or '').strip()
        inv_data = einv_map.get(bill_no, {})
        gstin    = inv_data.get('gstin','')
        rec_name = inv_data.get('name','')
        inv_type = inv_data.get('inv_type','')

        doc_type   = ''
        supply_type= ''
        if gstin:
            doc_type    = 'Invoice'
            supply_type = 'B2B'
        else:
            doc_type    = 'Invoice'
            supply_type = 'B2C'

        processed.append(base + [tax_head, rate, hsn_desc, hsn_code,
                                  rate_check, None, gstin, rec_name,
                                  inv_type, doc_type, supply_type])

    # Invoice Check
    bill_rc = defaultdict(float)
    bill_idx = 2; rc_idx = 18
    for row in processed:
        bill = str(row[bill_idx] or '')
        try: bill_rc[bill] += float(row[rc_idx] or 0)
        except: pass
    for row in processed:
        bill = str(row[bill_idx] or '')
        row[19] = 'OK' if abs(bill_rc.get(bill,0)) <= 50 else 'Error'

    return processed, new_hdrs

def make_proc_d110(d110_rows, mapping, einv_map):
    processed, new_hdrs = process_d110(d110_rows, mapping, einv_map)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Proc_D110'
    for c, h in enumerate(new_hdrs, 1):
        cell = ws.cell(1, c, h)
        cell.font = hdr_font(); cell.fill = hdr_fill()
    for r, row in enumerate(processed, 2):
        for c, v in enumerate(row, 1):
            ws.cell(r, c, v).font = data_font()
    set_heights(ws, len(processed)+1)
    ws.sheet_view.showGridLines = False
    return wb, processed, new_hdrs

def make_sales_register(processed, new_hdrs, einv_map):
    idx = {h:i for i,h in enumerate(new_hdrs)}
    i_gstin   = idx.get('GSTIN',20)
    i_recname = idx.get('Receiver Name',21)
    i_bill    = 2
    i_date    = 4
    i_hsndesc = idx.get('HSN Description',16)
    i_hsncode = idx.get('HSN Code',17)
    i_invtype = idx.get('Invoice Type',22)
    i_doctype = idx.get('Document Type',23)
    i_supply  = idx.get('Supply Type',24)
    i_rate    = idx.get('Rate',15)
    i_th      = idx.get('Tax Head',14)
    i_debit   = 10

    TAX_HEADS = ['IGST','CGST','SGST','CESS']
    pivot = defaultdict(lambda:{t:0.0 for t in TAX_HEADS})
    key_order = []; key_set = set()
    bill_rev_d110 = defaultdict(float)

    for row in processed:
        th = str(row[i_th] or '').strip().upper()
        gstin   = str(row[i_gstin]   or '').strip()
        recname = str(row[i_recname] or '').strip()
        bill    = str(row[i_bill]    or '').strip()
        date    = row[i_date]
        hsndesc = str(row[i_hsndesc] or '').strip()
        hsncode = row[i_hsncode]
        invtype = str(row[i_invtype] or '').strip()
        doctype = str(row[i_doctype] or '').strip()
        supply  = str(row[i_supply]  or '').strip()
        rate    = row[i_rate]
        try: debit = float(row[i_debit] or 0)
        except: debit = 0

        key = (gstin,recname,bill,date,hsndesc,hsncode,invtype,doctype,supply,rate)
        if key not in key_set:
            key_set.add(key); key_order.append(key)
        if th in TAX_HEADS:
            pivot[key][th] += debit
            bill_rev_d110[bill] += debit

    einv_bill_rev = {bill: d['taxable'] for bill, d in einv_map.items()}

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Sales_Register'

    pivot_hdrs = ['GSTIN','Receiver Name','BILL_NO','BILL_GENERATION_DATE',
                  'HSN Description','HSN Code','Invoice Type','Document Type',
                  'Supply Type','Rate','IGST','CGST','SGST','CESS',
                  'Revenue as per Taxes','Invoice Revenue','Revenue as per D110',
                  'Difference','Status']
    for c,h in enumerate(pivot_hdrs,1):
        cell = ws.cell(1,c,h); cell.font=hdr_font(); cell.fill=hdr_fill()

    DATA_START = 2
    for r,key in enumerate(key_order, DATA_START):
        gstin,recname,bill,date,hsndesc,hsncode,invtype,doctype,supply,rate = key
        vals = pivot[key]
        igst = vals['IGST'] if vals['IGST']!=0 else None
        cgst = vals['CGST'] if vals['CGST']!=0 else None
        sgst = vals['SGST'] if vals['SGST']!=0 else None
        cess = vals['CESS'] if vals['CESS']!=0 else None
        try:
            rate_num = float(rate) if rate else 0
            rev_tax = (sum(v for v in [vals['IGST'],vals['CGST'],vals['SGST'],vals['CESS']]) / rate_num) if rate_num else 0
        except: rev_tax = 0
        rev_d110 = bill_rev_d110.get(bill, 0)
        row_data = [gstin,recname,bill,date,hsndesc,hsncode,invtype,doctype,supply,rate,
                    igst,cgst,sgst,cess,rev_tax,0,rev_d110,0,'']
        for c,v in enumerate(row_data,1):
            cell = ws.cell(r,c,v); cell.font=data_font()
            if c in [11,12,13,14,15,16,17,18]: cell.number_format=FMT_COMMA

    LAST_DATA = DATA_START + len(key_order) - 1

    # Second pass — Invoice Revenue = SUMIF(bill, O:O)
    bill_inv_rev = defaultdict(float)
    for rn in range(DATA_START, LAST_DATA+1):
        b = str(ws.cell(rn,3).value or '')
        try: bill_inv_rev[b] += float(ws.cell(rn,15).value or 0)
        except: pass
    for rn in range(DATA_START, LAST_DATA+1):
        b   = str(ws.cell(rn,3).value or '')
        p   = bill_inv_rev.get(b,0)
        q   = ws.cell(rn,17).value or 0
        ws.cell(rn,16,p).number_format = FMT_COMMA
        try: diff = p - float(q or 0)
        except: diff = 0
        ws.cell(rn,18,diff).number_format = FMT_COMMA
        status = 'Passed' if abs(diff)<=50 else 'Pending'
        sc = ws.cell(rn,19,status)
        if status=='Passed': sc.font=Font(name='Calibri',size=10,color='FF0070C0')
        else: sc.font=Font(name='Calibri',size=10,color='FFC00000')

    TOTAL_ROW = LAST_DATA+1
    ws.cell(TOTAL_ROW,1,'TOTAL')
    for c in range(7,len(pivot_hdrs)+1):
        cl=get_column_letter(c)
        cell=ws.cell(TOTAL_ROW,c,f'=SUM({cl}{DATA_START}:{cl}{LAST_DATA})')
        cell.number_format=FMT_COMMA
    apply_total_row(ws,TOTAL_ROW,1,len(pivot_hdrs))
    set_heights(ws,TOTAL_ROW)
    ws.sheet_view.showGridLines=False
    return wb

def make_sr_no_gst(processed, new_hdrs):
    idx = {h:i for i,h in enumerate(new_hdrs)}
    i_th   = idx.get('Tax Head',14)
    i_rate = idx.get('Rate',15)
    i_bill = 2
    i_hsnd = idx.get('HSN Description',16)
    i_debit= 10

    pivot = defaultdict(float)
    bill_order=[]; bill_set=set()
    bill_hsn={}; bill_rate={}

    for row in processed:
        rate_str = str(row[i_rate] or '').strip().upper()
        if rate_str != 'NO GST': continue
        bill = str(row[i_bill] or '').strip()
        try: debit=float(row[i_debit] or 0)
        except: debit=0
        pivot[bill]+=debit
        if bill not in bill_set:
            bill_set.add(bill); bill_order.append(bill)
            bill_hsn[bill]=str(row[i_hsnd] or '').strip()
            bill_rate[bill]=row[i_rate]

    wb=openpyxl.Workbook(); ws=wb.active; ws.title='SR_No_GST'
    hdrs=['Bill No','Rate','HSN Description','Amount']
    for c,h in enumerate(hdrs,1):
        cell=ws.cell(1,c,h); cell.font=hdr_font(); cell.fill=hdr_fill()
    for r,bill in enumerate(bill_order,2):
        ws.cell(r,1,bill).font=data_font()
        ws.cell(r,2,bill_rate.get(bill)).font=data_font()
        ws.cell(r,3,bill_hsn.get(bill)).font=data_font()
        ws.cell(r,4,pivot[bill]).font=data_font()
        ws.cell(r,4).number_format=FMT_COMMA
    TOTAL_ROW=len(bill_order)+2
    ws.cell(TOTAL_ROW,1,'TOTAL')
    cell=ws.cell(TOTAL_ROW,4,f'=SUM(D2:D{TOTAL_ROW-1})')
    cell.number_format=FMT_COMMA
    apply_total_row(ws,TOTAL_ROW,1,4)
    set_heights(ws,TOTAL_ROW)
    ws.sheet_view.showGridLines=False
    return wb

def make_hsn_summary(processed, new_hdrs):
    idx={h:i for i,h in enumerate(new_hdrs)}
    i_th  =idx.get('Tax Head',14)
    i_rate=idx.get('Rate',15)
    i_hsnc=idx.get('HSN Code',17)
    i_hsnd=idx.get('HSN Description',16)
    i_dbt =10

    TAX=['IGST','CGST','SGST','CESS','REVENUE','NON GST']
    pivot=defaultdict(lambda:{t:0.0 for t in TAX})
    key_order=[]; key_set=set()

    for row in processed:
        th=str(row[i_th] or '').strip().upper()
        hsnc=row[i_hsnc]; hsnd=str(row[i_hsnd] or '').strip()
        rate=row[i_rate]
        try: dbt=float(row[i_dbt] or 0)
        except: dbt=0
        key=(hsnc,hsnd,rate)
        if key not in key_set: key_set.add(key); key_order.append(key)
        if th in TAX: pivot[key][th]+=dbt

    wb=openpyxl.Workbook(); ws=wb.active; ws.title='HSN_Summary'
    hdrs=['HSN Code','HSN Description','Rate','IGST','CGST','SGST','CESS','Revenue','NON GST']
    for c,h in enumerate(hdrs,1):
        cell=ws.cell(1,c,h); cell.font=hdr_font(); cell.fill=hdr_fill()

    DATA_START=2
    valid_keys=[k for k in key_order if not(k[0] is None and all(pivot[k][t]==0 for t in TAX))]
    for r,key in enumerate(valid_keys,DATA_START):
        hsnc,hsnd,rate=key; vals=pivot[key]
        try:
            rn=float(rate) if rate else 0
            rev=(vals['IGST']+vals['CGST']+vals['SGST']+vals['CESS'])/rn if rn else ''
        except: rev=''
        row_data=[hsnc,hsnd,rate,
                  vals['IGST'] or None,vals['CGST'] or None,vals['SGST'] or None,
                  vals['CESS'] or None,rev or None,vals['NON GST'] or None]
        for c,v in enumerate(row_data,1):
            cell=ws.cell(r,c,v); cell.font=data_font()
            if c==3: cell.number_format=FMT_PCT
            elif c>=4: cell.number_format=FMT_COMMA

    TOTAL_ROW=DATA_START+len(valid_keys)
    ws.cell(TOTAL_ROW,1,'TOTAL')
    for c in range(4,10):
        cl=get_column_letter(c)
        cell=ws.cell(TOTAL_ROW,c,f'=SUM({cl}{DATA_START}:{cl}{TOTAL_ROW-1})')
        cell.number_format=FMT_COMMA
    apply_total_row(ws,TOTAL_ROW,1,9)
    set_heights(ws,TOTAL_ROW)
    ws.sheet_view.showGridLines=False
    return wb, processed, new_hdrs

def make_d110_vs_gstr1(processed, new_hdrs):
    idx={h:i for i,h in enumerate(new_hdrs)}
    i_th=idx.get('Tax Head',14); i_dbt=10; i_hsnc=idx.get('HSN Code',17)

    gst_accounts={'217576':('CGST',),'217577':('SGST',),'217578':('IGST',),'217580':('CESS',)}
    TAX=['IGST','CGST','SGST','CESS']
    d110_tax={t:0.0 for t in TAX}; d110_rev=0.0; d110_nongst=0.0; d110_others=0.0

    hsn_wb,_,_ = make_hsn_summary(processed, new_hdrs)
    hsn_ws = hsn_wb.active
    hsn_rows = list(hsn_ws.iter_rows(values_only=True))
    total_row = next((r for r in hsn_rows if r[0]=='TOTAL'),None)

    gstr1_rev  = float(total_row[7] or 0) if total_row else 0
    gstr1_igst = float(total_row[3] or 0) if total_row else 0
    gstr1_cgst = float(total_row[4] or 0) if total_row else 0
    gstr1_sgst = float(total_row[5] or 0) if total_row else 0
    gstr1_cess = float(total_row[6] or 0) if total_row else 0

    for row in processed:
        th=str(row[i_th] or '').strip().upper()
        try: dbt=float(row[i_dbt] or 0)
        except: dbt=0
        if   th=='IGST':    d110_tax['IGST']+=dbt
        elif th=='CGST':    d110_tax['CGST']+=dbt
        elif th=='SGST':    d110_tax['SGST']+=dbt
        elif th=='CESS':    d110_tax['CESS']+=dbt
        elif th=='REVENUE': d110_rev+=dbt
        elif th=='NON GST': d110_nongst+=dbt
        elif th=='OTHERS':  d110_others+=dbt

    wb=openpyxl.Workbook(); ws=wb.active; ws.title='D110_Vs_GSTR1'
    ws.cell(4,1,'As per GSTR-1').font=bold_font()
    for c,v in enumerate([gstr1_rev,gstr1_igst,gstr1_cgst,gstr1_sgst,gstr1_cess],2):
        ws.cell(4,c,v).number_format=FMT_COMMA
    ws.cell(5,1,'As Per D110').font=bold_font()
    d110_total_rev = d110_rev
    for c,v in enumerate([d110_total_rev,d110_tax['IGST'],d110_tax['CGST'],d110_tax['SGST'],d110_tax['CESS']],2):
        ws.cell(5,c,v).number_format=FMT_COMMA
    ws.cell(6,1,'Differences').font=bold_font()
    for c in range(2,7):
        ws.cell(6,c,f'=B4-B5' if c==2 else f'={get_column_letter(c)}4-{get_column_letter(c)}5')
        ws.cell(6,c).number_format=FMT_COMMA
    ws.cell(9,1,'NON GST').font=data_font()
    ws.cell(9,2,d110_nongst).number_format=FMT_COMMA
    ws.cell(10,1,'OTHERS').font=data_font()
    ws.cell(10,2,d110_others).number_format=FMT_COMMA
    ws.cell(12,1,'TOTAL NO GST SUPPLIES').font=bold_font()
    ws.cell(12,2,'=B9+B10').number_format=FMT_COMMA
    ws.cell(14,1,'NET D110 VALUE').font=bold_font()
    ws.cell(14,2,'=B12+SUM(B5:F5)').number_format=FMT_COMMA
    ws.cell(16,1,'NET VALUE AS PER GSTR-1').font=bold_font()
    ws.cell(16,2,'=SUM(B4:F4)').number_format=FMT_COMMA
    ws.cell(18,1,'DIFFERENCES').font=bold_font()
    ws.cell(18,2,'=B14-B16').number_format=FMT_COMMA
    ws.cell(19,1,'NO GST SUPPLIES').font=data_font()
    ws.cell(19,2,'=-B12').number_format=FMT_COMMA
    ws.cell(20,1,'INCORRECT TAX CHARGED').font=data_font()
    ws.cell(20,2,'=B6').number_format=FMT_COMMA
    net_cell=ws.cell(21,1,'NET DIFFERENCE')
    net_cell.font=Font(name='Calibri',size=10,bold=True,color='FF0070C0')
    net_cell.fill=PatternFill('solid',fgColor='FFFFFF00')
    ws.cell(21,2,'=SUM(B18:B20)').number_format=FMT_COMMA
    ws.cell(21,2).font=Font(name='Calibri',size=10,bold=True,color='FF0070C0')
    set_heights(ws,21); ws.sheet_view.showGridLines=False
    return wb

def load_and_process():
    hotel_id = int(request.form.get('hotel_id', 0))
    # If hotel_id=0, get first available hotel
    if hotel_id == 0:
        conn = get_db()
        h = conn.execute("SELECT id FROM hotels LIMIT 1").fetchone()
        conn.close()
        hotel_id = h['id'] if h else 1
    # D110 — required
    d110_bytes = get_file_bytes(hotel_id, 'd110', 'd110')
    if not d110_bytes: raise ValueError('D110 Dr.Base file required — upload karo ya saved file use karo')
    # Backend — required
    back_bytes = get_file_bytes(hotel_id, 'backend', 'backend')
    if not back_bytes: raise ValueError('Backend Trx,SAC_Mapping required — upload karo ya saved file use karo')
    # EINV — optional
    einv_bytes = get_file_bytes(hotel_id, 'einv', 'einv')

    d110_rows    = get_sheet(read_wb(d110_bytes), 'D110 Dr.Base')
    backend_rows = get_sheet(read_wb(back_bytes), 'Backend Trx,SAC_Mapping')
    gstr_rows    = get_sheet(read_wb(einv_bytes), 'GSTR-4A_EINV') if einv_bytes else []
    mapping      = build_map(backend_rows)
    einv_map     = build_einv_map(gstr_rows)
    processed, new_hdrs = process_d110(d110_rows, mapping, einv_map)
    return processed, new_hdrs, einv_map

# ══════════════════════════════════════════════════════════
# D110 ROUTES
# ══════════════════════════════════════════════════════════
@app.route('/process/step1', methods=['POST'])
@login_required
def step1():
    try:
        d110f=request.files.get('d110'); backf=request.files.get('backend'); einvf=request.files.get('einv')
        if not d110f or not backf: return jsonify({'error':'D110 + Backend required'}),400
        d110_rows=get_sheet(read_wb(d110f.read()),'D110 Dr.Base')
        backend_rows=get_sheet(read_wb(backf.read()),'Backend Trx,SAC_Mapping')
        gstr_rows=get_sheet(read_wb(einvf.read()),'GSTR-4A_EINV') if einvf else []
        mapping=build_map(backend_rows); einv_map=build_einv_map(gstr_rows)
        wb,_,_=make_proc_d110(d110_rows,mapping,einv_map)
        save_history('D110','Step 1 - Proc D110','Proc_D110.xlsx')
        return send_wb(wb,'Proc_D110.xlsx')
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error':str(e)}),500

@app.route('/process/step2', methods=['POST'])
@login_required
def step2():
    try:
        processed,new_hdrs,einv_map=load_and_process()
        wb=make_sales_register(processed,new_hdrs,einv_map)
        save_history('D110','Step 2 - Sales Register','Sales_Register.xlsx')
        return send_wb(wb,'Sales_Register.xlsx')
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error':str(e)}),500

@app.route('/process/step3', methods=['POST'])
@login_required
def step3():
    try:
        processed,new_hdrs,_=load_and_process()
        wb=make_sr_no_gst(processed,new_hdrs)
        save_history('D110','Step 3 - SR No GST','SR_No_GST.xlsx')
        return send_wb(wb,'SR_No_GST.xlsx')
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error':str(e)}),500

@app.route('/process/step4', methods=['POST'])
@login_required
def step4():
    try:
        processed,new_hdrs,_=load_and_process()
        wb,_,_=make_hsn_summary(processed,new_hdrs)
        save_history('D110','Step 4 - HSN Summary','HSN_Summary.xlsx')
        return send_wb(wb,'HSN_Summary.xlsx')
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error':str(e)}),500

@app.route('/process/step5', methods=['POST'])
@login_required
def step5():
    try:
        processed,new_hdrs,_=load_and_process()
        wb=make_d110_vs_gstr1(processed,new_hdrs)
        save_history('D110','Step 5 - D110 vs GSTR1','D110_Vs_GSTR1_Reco.xlsx')
        return send_wb(wb,'D110_Vs_GSTR1_Reco.xlsx')
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error':str(e)}),500

# ══════════════════════════════════════════════════════════
# D140 FUNCTIONS
# ══════════════════════════════════════════════════════════
def build_d140_mapping(backend_rows):
    mapping={}
    for row in backend_rows[2:]:
        if not row[1]: continue
        key=str(row[1] or '').strip().lower()
        mapping[key]={'tax_head':row[2],'rate':row[3],'hsn_desc':row[4],'hsn_code':row[5],'bof_code':row[6]}
    return mapping

def process_d140(d140_rows, mapping):
    KEEP=34
    headers=list(d140_rows[0])[:KEEP]+['Tax Head','Rate','HSN Description','HSN Code','BOF Code','Rate Check','Status','Guest+Room']
    processed=[]
    I_TRX_DESC=8; I_GUEST=6; I_ROOM=17; I_PRINT_DR=33
    for row in d140_rows[1:]:
        if not any(row): continue
        base=list(row)[:KEEP]
        while len(base)<KEEP: base.append(None)
        trx_desc=str(base[I_TRX_DESC] or '').strip().lower()
        m=mapping.get(trx_desc,{})
        tax_head=m.get('tax_head'); rate=m.get('rate')
        hsn_desc=m.get('hsn_desc'); hsn_code=m.get('hsn_code'); bof_code=m.get('bof_code')
        try: ah_val=float(base[I_PRINT_DR] or 0)
        except: ah_val=0
        rate_str=str(rate).strip().upper() if rate is not None else ''
        if rate_str=='NO GST': rc=0
        elif rate_str in ('','0','NONE') or rate is None or rate==0: rc=ah_val*-1
        else:
            try:
                rn=float(rate); rc=ah_val/rn if rn!=0 else ah_val*-1
            except: rc=ah_val*-1
        guest=str(base[I_GUEST] or '').strip(); room=str(base[I_ROOM] or '').strip()
        gr=f"{guest}-{room}"
        processed.append(base+[tax_head,rate,hsn_desc,hsn_code,bof_code,rc,None,gr])
    dict_sum=defaultdict(float)
    for row in processed:
        try: dict_sum[row[-1]]+=float(row[-3] or 0)
        except: pass
    for row in processed:
        row[-2]='Passed' if abs(dict_sum.get(row[-1],0))<=50 else 'Error'
    return processed, headers

def make_proc_d140(d140_rows, backend_rows):
    mapping=build_d140_mapping(backend_rows)
    processed,headers=process_d140(d140_rows,mapping)
    wb=openpyxl.Workbook(); ws=wb.active; ws.title='Proc_D140'
    for c,h in enumerate(headers,1):
        cell=ws.cell(1,c,h); cell.font=hdr_font(); cell.fill=hdr_fill()
    for r,row in enumerate(processed,2):
        for c,v in enumerate(row,1):
            cell=ws.cell(r,c,v); cell.font=data_font()
            if c==40: cell.number_format=FMT_COMMA
    set_heights(ws,len(processed)+1); ws.sheet_view.showGridLines=False
    return wb,processed,headers

def make_gst_pivot(processed, headers):
    idx={h:i for i,h in enumerate(headers)}
    TAX_SHOW={'IGST','CGST','SGST','CESS'}; TAX_COLS=['IGST','CGST','SGST','CESS']
    pivot=defaultdict(lambda:{t:0.0 for t in TAX_COLS}); key_set=set()
    for row in processed:
        th=str(row[idx.get('Tax Head',34)] or '').strip().upper()
        if th not in TAX_SHOW: continue
        key=(row[idx.get('HSN Code',37)],str(row[idx.get('HSN Description',36)] or '').strip(),row[idx.get('Rate',35)])
        key_set.add(key)
        try: val=float(row[idx.get('PRINT_CASHIER_DEBIT',33)] or 0)
        except: val=0
        pivot[key][th]+=val
    key_order=sorted(key_set,key=lambda k:(int(k[0]) if k[0] else 0,str(k[1]),float(k[2] or 0)))
    wb=openpyxl.Workbook(); ws=wb.active; ws.title='GST_Pivot'
    ws.cell(1,1,'Sum of PRINT_CASHIER_DEBIT').font=data_font()
    ws.cell(1,4,'Tax Head').font=data_font()
    for c,h in enumerate(['HSN Code','HSN Description','Rate','IGST','CGST','SGST','CESS','Revenue'],1):
        cell=ws.cell(2,c,h); cell.font=Font(name='Calibri',size=10,bold=True,color='FFFFFFFF'); cell.fill=hdr_fill()
    DATA_START=3
    for r,key in enumerate(key_order,DATA_START):
        hc,hd,rate=key; vals=pivot[key]
        igst=vals['IGST'] or None; cgst=vals['CGST'] or None; sgst=vals['SGST'] or None; cess=vals['CESS'] or None
        try: rev=(vals['IGST']+vals['CGST']+vals['SGST'])/float(rate) if rate else None
        except: rev=None
        for c,v in enumerate([hc,hd,rate,igst,cgst,sgst,cess,rev],1):
            cell=ws.cell(r,c,v); cell.font=data_font()
            if c>=4: cell.number_format=FMT_COMMA
    DATA_END=DATA_START+len(key_order)-1; TOTAL_ROW=DATA_END+1
    ws.cell(TOTAL_ROW,1,'Total').font=Font(name='Calibri',size=10,bold=True,color='FFFFFFFF'); ws.cell(TOTAL_ROW,1).fill=hdr_fill()
    for c in range(4,9):
        cl=get_column_letter(c); cell=ws.cell(TOTAL_ROW,c,f'=SUM({cl}{DATA_START}:{cl}{DATA_END})')
        cell.font=Font(name='Calibri',size=10,bold=True,color='FFFFFFFF'); cell.fill=hdr_fill(); cell.number_format=FMT_COMMA
    set_heights(ws,TOTAL_ROW); ws.sheet_view.showGridLines=False
    return wb

def _get_ledger_info(acc_code,acc_desc,user_batch,trx_desc):
    acc_code=str(acc_code or '').strip(); acc_desc=str(acc_desc or '').strip().upper()
    user_batch=str(user_batch or '').strip(); trx_desc=str(trx_desc or '').strip().upper()
    gst_map={'217576':('GST Payable Ledger','Output Central GST'),'217577':('GST Payable Ledger','Output State GST'),
             '217578':('GST Payable Ledger','Output Integrated GST'),'217580':('GST Payable Ledger','Output Cess'),
             '217586':('GST Payable Ledger','Output RCM Union GST Liab'),'137506':('GST Receivable Ledger','Input Central GST'),
             '137507':('GST Receivable Ledger','Input State GST'),'137508':('GST Receivable Ledger','Input Integrated GST'),
             '137528':('GST Receivable Ledger','Input RCM - Integrated GST')}
    lt=''; ln=''
    if acc_code in gst_map: lt,ln=gst_map[acc_code]
    elif acc_code.startswith('3'): lt='Revenue Ledger'; ln='Revenue Ledger'
    elif acc_code: lt='Other than GST'; ln='Other than GST'
    gst_kw=('GST','TAX','JV','ENTRY','CORRECT','PAYMENT','ADJUST')
    rm=''
    if lt=='Revenue Ledger': rm='Revenue-Non GST' if any(w in acc_desc for w in ('BEER','WINE','LIQUOR')) else 'Revenue Booked'
    elif lt=='GST Payable Ledger': rm=ln if user_batch else 'GST Payment & Adjustment'
    elif lt=='GST Receivable Ledger': rm='GST Payment & Adjustment' if any(w in trx_desc for w in gst_kw) else 'GST Input Credit'
    elif lt=='Other than GST': rm='Other than GST'
    return lt,ln,rm

def make_proc_gl(gl_rows):
    GL_ORIG_COLS=38
    data_rows=[]
    for row in gl_rows[1:]:
        if not any(row): continue
        base=list(row)[:GL_ORIG_COLS]
        while len(base)<GL_ORIG_COLS: base.append(None)
        lt,ln,rm=_get_ledger_info(base[6],base[7],base[15],base[20])
        data_rows.append(base+[lt,ln,rm,None,None])
    wb=openpyxl.Workbook(); ws=wb.active; ws.title='Proc GL'
    DATA_START=7; DATA_END=DATA_START+len(data_rows)-1
    ws.cell(1,1,'GL Summary - Proc GL').font=Font(name='Calibri',size=12,bold=True)
    ws.cell(3,1,'Sub Total').font=Font(name='Calibri',size=10,bold=True,color='FFFFFFFF'); ws.cell(3,1).fill=hdr_fill()
    ws.cell(4,1,'Total').font=Font(name='Calibri',size=10,bold=True,color='FFFFFFFF'); ws.cell(4,1).fill=hdr_fill()
    for col in [26,30]:
        cl=get_column_letter(col)
        for rn,formula in [(3,f'=SUBTOTAL(9,{cl}{DATA_START}:{cl}{DATA_END})'),(4,f'=SUM({cl}{DATA_START}:{cl}{DATA_END})')]:
            c=ws.cell(rn,col,formula); c.font=Font(name='Calibri',size=10,bold=True,color='FFFFFFFF')
            c.fill=hdr_fill(); c.number_format=FMT_COMMA
    orig_hdrs=list(gl_rows[0])[:GL_ORIG_COLS]
    while len(orig_hdrs)<GL_ORIG_COLS: orig_hdrs.append(None)
    all_hdrs=orig_hdrs+['Ledger Type','Ledger Name','LTD Balance','Opera/Manual','Remarks']
    for c,h in enumerate(all_hdrs,1):
        cell=ws.cell(6,c,h); cell.font=Font(name='Calibri',size=10,bold=True,color='FFFFFFFF'); cell.fill=hdr_fill()
    for r,row in enumerate(data_rows,DATA_START):
        for c,v in enumerate(row,1): ws.cell(r,c,v).font=data_font()
    set_heights(ws,DATA_END); ws.sheet_view.showGridLines=False
    return wb,gl_rows

def make_gl_summary(gl_rows):
    lt_order={'GST Payable Ledger':0,'GST Receivable Ledger':1,'Other than GST':2,'Revenue Ledger':3}
    pivot=defaultdict(lambda:defaultdict(float)); key_set=set()
    for row in gl_rows[1:]:
        if not any(row): continue
        lt,ln,rm=_get_ledger_info(row[6],row[7],row[15],row[20])
        if not lt: continue
        om=str(row[14] or '').strip() or '(blank)'
        try: amt=float(row[29] or 0)
        except: amt=0
        key=(lt,ln,rm,None); key_set.add(key); pivot[key][om]+=amt
    key_order=sorted(key_set,key=lambda k:(lt_order.get(k[0],9),k[1],k[2]))
    om_cols=sorted(set(k for v in pivot.values() for k in v.keys()))
    wb=openpyxl.Workbook(); ws=wb.active; ws.title='GL Summary'
    ws.cell(3,1,'Sum of Base Amount').font=data_font()
    ws.cell(3,5,'Opera/Manual').font=data_font()
    hdr_vals=['Ledger Type','Ledger Name','Remarks','LTD Balance']+om_cols+['Grand Total']
    for c,h in enumerate(hdr_vals,1):
        cell=ws.cell(4,c,h); cell.font=Font(name='Calibri',size=10,bold=True,color='FFFFFFFF'); cell.fill=hdr_fill()
    DATA_START=5
    for r,key in enumerate(key_order,DATA_START):
        lt,ln,rm,ltd=key
        for c,v in enumerate([lt,ln,rm,ltd],1): ws.cell(r,c,v).font=data_font()
        rt=0
        for c,om in enumerate(om_cols,5):
            val=pivot[key].get(om,0) or None
            cell=ws.cell(r,c,val); cell.font=data_font(); cell.number_format=FMT_COMMA
            rt+=pivot[key].get(om,0)
        gt=ws.cell(r,5+len(om_cols),rt or None); gt.font=data_font(); gt.number_format=FMT_COMMA
    DATA_END=DATA_START+len(key_order)-1; TOTAL_ROW=DATA_END+1
    ws.cell(TOTAL_ROW,1,'Grand Total').font=Font(name='Calibri',size=10,bold=True,color='FFFFFFFF'); ws.cell(TOTAL_ROW,1).fill=hdr_fill()
    for c in range(5,6+len(om_cols)):
        cl=get_column_letter(c); cell=ws.cell(TOTAL_ROW,c,f'=SUM({cl}{DATA_START}:{cl}{DATA_END})')
        cell.font=Font(name='Calibri',size=10,bold=True,color='FFFFFFFF'); cell.fill=hdr_fill(); cell.number_format=FMT_COMMA
    set_heights(ws,TOTAL_ROW); ws.sheet_view.showGridLines=False
    return wb

def make_opera_vs_gl(processed, headers, gl_rows):
    idx={h:i for i,h in enumerate(headers)}
    i_date=idx.get('BUSINESS_DATE',12); i_dr=idx.get('CASHIER_DEBIT',15); i_th=idx.get('Tax Head',34)
    opera_by_date=defaultdict(lambda:defaultdict(float))
    for row in processed:
        date=str(row[i_date] or '').strip(); th=str(row[i_th] or '').strip().upper()
        try: dr=float(row[i_dr] or 0)
        except: dr=0
        if date: opera_by_date[date][th]+=dr
    gl_by_date=defaultdict(float)
    for row in gl_rows[1:]:
        if not any(row): continue
        if str(row[14] or '').strip()!='SWN': continue
        try: amt=float(row[29] or 0)
        except: amt=0
        jdate=row[18]
        if jdate:
            d=str(jdate)[:10] if hasattr(jdate,'strftime') else str(jdate)[:10]
            gl_by_date[d]+=amt
    wb=openpyxl.Workbook(); ws=wb.active; ws.title='Opera_VS_GL'
    hdrs=['Date','As per D140 (Opera) - Revenue','As per D140 - Total Tax','As per D140 - Total','As per GL (SWN)','Difference','Remark']
    for c,h in enumerate(hdrs,1):
        cell=ws.cell(1,c,h); cell.font=hdr_font(); cell.fill=hdr_fill()
    all_dates=sorted(set(list(opera_by_date.keys())+list(gl_by_date.keys())))
    for r,date in enumerate(all_dates,2):
        d140_rev=opera_by_date[date].get('REVENUE',0)
        d140_tax=sum(v for k,v in opera_by_date[date].items() if k in ('IGST','CGST','SGST','CESS'))
        d140_total=sum(opera_by_date[date].values())
        gl_total=gl_by_date.get(date,0)
        diff=d140_total-gl_total
        remark='OK' if abs(diff)<=50 else 'Check'
        vals=[date,d140_rev or None,d140_tax or None,d140_total or None,gl_total or None,diff or None,remark]
        for c,v in enumerate(vals,1):
            cell=ws.cell(r,c,v); cell.font=data_font()
            if c in [2,3,4,5,6]: cell.number_format=FMT_COMMA
            if c==7:
                if v=='OK': cell.font=Font(name='Calibri',size=10,color='FF0070C0')
                else: cell.font=Font(name='Calibri',size=10,color='FFC00000')
    TOTAL_ROW=len(all_dates)+2
    ws.cell(TOTAL_ROW,1,'TOTAL').font=bold_font(); ws.cell(TOTAL_ROW,1).fill=hdr_fill()
    for c in [2,3,4,5,6]:
        cl=get_column_letter(c); cell=ws.cell(TOTAL_ROW,c,f'=SUM({cl}2:{cl}{TOTAL_ROW-1})')
        cell.font=bold_font(); cell.fill=hdr_fill(); cell.number_format=FMT_COMMA
    set_heights(ws,TOTAL_ROW); ws.sheet_view.showGridLines=False
    return wb

# ══════════════════════════════════════════════════════════
# D140 ROUTES
# ══════════════════════════════════════════════════════════
def load_d140():
    hotel_id = int(request.form.get('hotel_id', 0))
    if hotel_id == 0:
        conn = get_db()
        h = conn.execute("SELECT id FROM hotels LIMIT 1").fetchone()
        conn.close()
        hotel_id = h['id'] if h else 1
    d140_bytes = get_file_bytes(hotel_id, 'd140', 'd140')
    back_bytes = get_file_bytes(hotel_id, 'backend', 'backend')
    if not d140_bytes or not back_bytes: return None,None,None,'D140 + Backend required'
    d140_rows    = get_sheet(read_wb(d140_bytes), 'D140 Dr')
    backend_rows = get_sheet(read_wb(back_bytes), 'Backend Trx,SAC_Mapping')
    return d140_rows,backend_rows,None,None

@app.route('/process/d140/step1', methods=['POST'])
@login_required
def d140_step1():
    try:
        d140_rows,backend_rows,_,err=load_d140()
        if err: return jsonify({'error':err}),400
        wb,_,_=make_proc_d140(d140_rows,backend_rows)
        save_history('D140','Step 1 - Proc D140','Proc_D140.xlsx')
        return send_wb(wb,'Proc_D140.xlsx')
    except Exception as e:
        import traceback; traceback.print_exc(); return jsonify({'error':str(e)}),500

@app.route('/process/d140/step2', methods=['POST'])
@login_required
def d140_step2():
    try:
        d140_rows,backend_rows,_,err=load_d140()
        if err: return jsonify({'error':err}),400
        mapping=build_d140_mapping(backend_rows)
        processed,headers=process_d140(d140_rows,mapping)
        wb=make_gst_pivot(processed,headers)
        save_history('D140','Step 2 - GST Pivot','GST_Pivot.xlsx')
        return send_wb(wb,'GST_Pivot.xlsx')
    except Exception as e:
        import traceback; traceback.print_exc(); return jsonify({'error':str(e)}),500

@app.route('/process/d140/step3', methods=['POST'])
@login_required
def d140_step3():
    try:
        glf=request.files.get('gl')
        if not glf: return jsonify({'error':'GL file required'}),400
        gl_rows=get_sheet(read_wb(glf.read()),'GL')
        wb,_=make_proc_gl(gl_rows)
        save_history('D140','Step 3 - Proc GL','Proc_GL.xlsx')
        return send_wb(wb,'Proc_GL.xlsx')
    except Exception as e:
        import traceback; traceback.print_exc(); return jsonify({'error':str(e)}),500

@app.route('/process/d140/step4', methods=['POST'])
@login_required
def d140_step4():
    try:
        glf=request.files.get('gl')
        if not glf: return jsonify({'error':'GL file required'}),400
        gl_rows=get_sheet(read_wb(glf.read()),'GL')
        wb=make_gl_summary(gl_rows)
        save_history('D140','Step 4 - GL Summary','GL_Summary.xlsx')
        return send_wb(wb,'GL_Summary.xlsx')
    except Exception as e:
        import traceback; traceback.print_exc(); return jsonify({'error':str(e)}),500

@app.route('/process/d140/step5', methods=['POST'])
@login_required
def d140_step5():
    try:
        d140f=request.files.get('d140'); backf=request.files.get('backend'); glf=request.files.get('gl')
        if not d140f or not backf or not glf: return jsonify({'error':'D140 + Backend + GL required'}),400
        d140_rows=get_sheet(read_wb(d140f.read()),'D140 Dr')
        backend_rows=get_sheet(read_wb(backf.read()),'Backend Trx,SAC_Mapping')
        gl_rows=get_sheet(read_wb(glf.read()),'GL')
        mapping=build_d140_mapping(backend_rows)
        processed,headers=process_d140(d140_rows,mapping)
        wb=make_opera_vs_gl(processed,headers,gl_rows)
        save_history('D140','Step 5 - Opera VS GL Reco','Opera_VS_GL_Reco.xlsx')
        return send_wb(wb,'Opera_VS_GL_Reco.xlsx')
    except Exception as e:
        import traceback; traceback.print_exc(); return jsonify({'error':str(e)}),500

# ══════════════════════════════════════════════════════════
# INIT + RUN
# ══════════════════════════════════════════════════════════
init_db()

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

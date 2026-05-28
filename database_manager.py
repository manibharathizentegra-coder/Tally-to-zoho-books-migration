import sqlite3
import os
import atexit
import contextvars

DEFAULT_DB_NAME = "tally_data.db"
_DB_NAME_VAR = contextvars.ContextVar("db_name", default=DEFAULT_DB_NAME)

_WRITE_CONN = {}  # db_name -> sqlite3.Connection

def close_write_connection():
    global _WRITE_CONN
    for conn in list(_WRITE_CONN.values()):
        try:
            conn.close()
        except Exception:
            pass
    _WRITE_CONN = {}

atexit.register(close_write_connection)

def set_active_db(db_name: str):
    """
    Set the active DB for the current context (request/session).
    db_name should be a filename like 'tally_data.db' (no directories).
    """
    if not db_name:
        db_name = DEFAULT_DB_NAME
    _DB_NAME_VAR.set(db_name)

def get_active_db() -> str:
    return _DB_NAME_VAR.get()

def get_db_connection(write=False, db_name=None):
    global _WRITE_CONN
    if db_name:
        set_active_db(db_name)

    db_to_use = get_active_db()

    if write:
        if db_to_use not in _WRITE_CONN or _WRITE_CONN[db_to_use] is None:
            _WRITE_CONN[db_to_use] = sqlite3.connect(
                db_to_use,
                timeout=60,
                isolation_level=None,  # autocommit
                check_same_thread=False
            )
            _WRITE_CONN[db_to_use].row_factory = sqlite3.Row
        return _WRITE_CONN[db_to_use]

    conn = sqlite3.connect(db_to_use, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_name=None):
    if db_name:
        set_active_db(db_name)
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Enable WAL ONCE (no retry, no loop)
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")

    # GROUPS TABLE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            parent TEXT,
            primary_group TEXT
        )
    ''')

    # ZOHO ITEM SYNC MAP (for fast resume / delta sync)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS zoho_item_sync (
            item_name TEXT PRIMARY KEY,
            zoho_item_id TEXT,
            signature TEXT,
            last_synced_at TEXT
        )
    ''')

    # INVENTORY ADJUSTMENT APPLY RESUME (run_id -> applied Zoho item_ids)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS inv_adj_applied (
            run_id TEXT,
            item_id TEXT,
            item_name TEXT,
            applied_at TEXT,
            PRIMARY KEY (run_id, item_id)
        )
    ''')
    
    # LEDGERS TABLE
    # Expanded to include all fields found in ledgers_backend.py
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ledgers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            parent TEXT,
            type TEXT, -- 'customer', 'vendor', 'other'
            
            address TEXT,
            state TEXT,
            country TEXT,
            pincode TEXT,
            email TEXT,
            phone TEXT,
            
            gstin TEXT,
            gst_reg_type TEXT,
            pan TEXT,
            
            opening_balance REAL,
            closing_balance REAL,
            
            description TEXT
        )
    ''')
    
    # ITEMS TABLE
    # Expanded to include all fields found in items_backend.py
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            group_name TEXT,
            category TEXT, -- Added category field
            unit TEXT,
            
            hsn_source TEXT,
            hsn TEXT,
            description TEXT,
            
            gst_applicable TEXT,
            gst_rate_source TEXT,
            gst_rate REAL,
            taxability TEXT,
            supply_type TEXT,
            rate_of_duty REAL,
            
            qty REAL,
            qty_unit TEXT,
            rate REAL,
            rate_unit TEXT,
            value REAL
        )
    ''')
    
    # COST CATEGORIES
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cost_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            allocate_revenue TEXT,
            allocate_non_revenue TEXT
        )
    ''')

    # COST CENTRES
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cost_centres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            category TEXT,
            parent TEXT
        )
    ''')
    
    # RECEIPTS (PAYMENT RECEIVED) - Expanded to match Tally fields
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS receipts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            receipt_number TEXT UNIQUE,
            voucher_type TEXT,
            date TEXT,
            
            -- Customer/Party Details
            customer_name TEXT,
            customer_ledger_amount REAL,
            
            -- Payment Details
            payment_mode TEXT,
            bank_account TEXT,
            account_current_balance REAL,
            amount REAL,
            reference_number TEXT,
            against_reference TEXT,
            
            -- Narration
            narration TEXT,
            
            -- Allocations (JSON strings)
            invoice_allocations TEXT,  -- JSON array of invoice allocations
            ledger_entries TEXT,       -- JSON array of all ledger entries
            cost_center_allocations TEXT,  -- JSON array of cost center details
            
            -- Rounding
            rounding_amount REAL,
            rounding_ledger TEXT,
            
            -- System Fields
            tally_guid TEXT,
            company_name TEXT,
            
            -- Timestamps
            created_at TEXT,
            updated_at TEXT
        )
    ''')

    # Prefer tally_guid as the durable unique key (receipt_number can change across exports).
    # Partial unique index allows multiple empty GUIDs.
    try:
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_receipts_tally_guid ON receipts(tally_guid) WHERE tally_guid != ''")
    except Exception:
        pass
    
    # PAYMENTS MADE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS payments_made (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payment_number TEXT UNIQUE,
            voucher_type TEXT,
            date TEXT,
            
            -- Vendor/Party Details
            vendor_name TEXT,
            vendor_ledger_amount REAL,
            
            -- Payment Details
            payment_mode TEXT,
            bank_account TEXT,
            account_current_balance REAL,
            amount REAL,
            reference_number TEXT,
            against_reference TEXT,
            
            -- Narration
            narration TEXT,
            
            -- Allocations (JSON strings)
            bill_allocations TEXT,  -- JSON array of bill allocations
            ledger_entries TEXT,       -- JSON array of all ledger entries
            cost_center_allocations TEXT,  -- JSON array of cost center details
            
            -- Rounding
            rounding_amount REAL,
            rounding_ledger TEXT,
            
            -- System Fields
            tally_guid TEXT,
            company_name TEXT,
            
            -- Timestamps
            created_at TEXT,
            updated_at TEXT
        )
    ''')

    # JOURNALS TABLE
    # Stores every journal voucher fetched from Tally (flat design — line_items as JSON)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS journals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Voucher identity
            journal_number TEXT UNIQUE,
            date TEXT,
            narration TEXT,

            -- Computed totals (derived from line_items at save time)
            total_debit  REAL DEFAULT 0,
            total_credit REAL DEFAULT 0,

            -- Full line-item detail stored as JSON array.
            -- Each element: { ledger_name, ledger_type, amount,
            --                 debit_or_credit, tag_category, tag_option }
            line_items TEXT,

            -- Date range the voucher was fetched for (helpful for re-fetch detection)
            from_date TEXT,
            to_date   TEXT,

            -- Timestamps
            created_at TEXT,
            updated_at TEXT
        )
    ''')

    # Index on date for fast range queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_journals_date ON journals(date)')

    # INVOICES TABLE
    # Stores every invoice voucher fetched from Tally — all fields, no exceptions
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Voucher identity
            invoice_number TEXT UNIQUE,
            date           TEXT,
            customer_name  TEXT,

            -- Header fields
            po_number      TEXT,         -- BASICPURCHASEORDERNO
            buyer_address  TEXT,         -- JSON array of address lines
            payment_terms  TEXT,         -- e.g. "30 Days"
            sales_ledger   TEXT,         -- identified sales ledger name
            narration      TEXT,

            -- e-Invoice / IRN fields
            irn            TEXT,
            irn_ack_no     TEXT,
            irn_ack_date   TEXT,

            -- Line items stored as JSON array
            -- Each element: { item_name, quantity, rate, discount,
            --                 amount, category, cost_centre }
            line_items     TEXT,

            -- Tax details stored as JSON array
            -- Each element: { tax_name, tax_type, tax_rate, tax_amount }
            taxes          TEXT,

            -- Totals
            rounding_off   REAL DEFAULT 0,
            subtotal       REAL DEFAULT 0,
            tax_total      REAL DEFAULT 0,
            total_amount   REAL DEFAULT 0,

            -- Date range it was fetched for
            from_date      TEXT,
            to_date        TEXT,

            -- Timestamps
            created_at     TEXT,
            updated_at     TEXT
        )
    ''')

    # Indexes for invoices
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_invoices_date          ON invoices(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_invoices_customer_name ON invoices(customer_name)')

    # BILLS TABLE
    # Stores every Purchase bill fetched from Tally — all fields, no exceptions
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Voucher identity
            bill_number    TEXT UNIQUE,
            date           TEXT,
            vendor_name    TEXT,

            -- Header fields
            po_number        TEXT,         -- BASICPURCHASEORDERNO
            reference_number TEXT,         -- REFERENCE (Vendor Invoice Number)
            vendor_address   TEXT,         -- JSON array of address lines
            payment_terms    TEXT,         -- e.g. "30 Days"
            purchase_ledger  TEXT,         -- identified purchase ledger name
            narration        TEXT,

            -- Line items stored as JSON array
            -- Each element: { item_name, quantity, rate, discount,
            --                 amount, category, cost_centre }
            line_items     TEXT,

            -- Tax details stored as JSON array
            -- Each element: { tax_name, tax_type, tax_rate, tax_amount }
            taxes          TEXT,

            -- Totals
            rounding_off   REAL DEFAULT 0,
            subtotal       REAL DEFAULT 0,
            tax_total      REAL DEFAULT 0,
            total_amount   REAL DEFAULT 0,

            -- Date range it was fetched for
            from_date      TEXT,
            to_date        TEXT,

            -- Timestamps
            created_at     TEXT,
            updated_at     TEXT
        )
    ''')

    # Indexes for bills
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bills_date        ON bills(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bills_vendor_name ON bills(vendor_name)')

    # SALES ORDERS TABLE
    # Stores every Sales Order fetched from Tally — all fields, no exceptions
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Voucher identity
            sales_order_number TEXT UNIQUE,
            date               TEXT,
            customer_name      TEXT,

            -- Header fields
            reference_number  TEXT,         -- REFERENCE (customer PO number)
            customer_address  TEXT,         -- JSON array of address lines
            payment_terms     TEXT,
            order_status      TEXT,         -- ORDERSTATUS
            sales_ledger      TEXT,         -- identified sales ledger name
            narration         TEXT,

            -- Line items stored as JSON array
            -- Each element: { item_name, quantity, rate, discount, amount }
            line_items        TEXT,

            -- Tax details stored as JSON array
            -- Each element: { tax_name, tax_type, tax_rate, tax_amount }
            taxes             TEXT,

            -- Totals
            rounding_off      REAL DEFAULT 0,
            subtotal          REAL DEFAULT 0,
            tax_total         REAL DEFAULT 0,
            total_amount      REAL DEFAULT 0,

            -- Date range it was fetched for
            from_date         TEXT,
            to_date           TEXT,

            -- Timestamps
            created_at        TEXT,
            updated_at        TEXT
        )
    ''')

    # Indexes for sales_orders
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sales_orders_date          ON sales_orders(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sales_orders_customer_name ON sales_orders(customer_name)')

    # PURCHASE ORDERS TABLE
    # Stores every Purchase Order fetched from Tally — all fields, no exceptions
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS purchase_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Voucher identity
            purchase_order_number TEXT UNIQUE,
            date                  TEXT,
            vendor_name           TEXT,

            -- Header fields
            reference_number  TEXT,         -- REFERENCE (vendor PO number)
            vendor_address    TEXT,         -- JSON array of address lines
            payment_terms     TEXT,
            order_status      TEXT,         -- ORDERSTATUS
            purchase_ledger   TEXT,         -- identified purchase ledger name
            narration         TEXT,

            -- Line items stored as JSON array
            -- Each element: { item_name, quantity, rate, discount,
            --                 amount, category, cost_centre }
            line_items        TEXT,

            -- Tax details stored as JSON array
            -- Each element: { tax_name, tax_type, tax_rate, tax_amount }
            taxes             TEXT,

            -- Totals
            rounding_off      REAL DEFAULT 0,
            subtotal          REAL DEFAULT 0,
            tax_total         REAL DEFAULT 0,
            total_amount      REAL DEFAULT 0,

            -- Date range it was fetched for
            from_date         TEXT,
            to_date           TEXT,

            -- Timestamps
            created_at        TEXT,
            updated_at        TEXT
        )
    ''')

    # Indexes for purchase_orders
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_purchase_orders_date        ON purchase_orders(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_purchase_orders_vendor_name ON purchase_orders(vendor_name)')

    # CONTRA VOUCHERS TABLE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS contra_vouchers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Voucher identity
            contra_number     TEXT UNIQUE,
            voucher_type      TEXT,
            date              TEXT,

            -- Account details
            from_account      TEXT,
            to_account        TEXT,
            amount            REAL DEFAULT 0,
            narration         TEXT,

            -- Ledger entries JSON (snapshot of all entries)
            ledger_entries    TEXT,

            -- Cost center allocations JSON
            cost_center_allocations TEXT,

            -- Tally ID mapping
            tally_guid        TEXT,
            company_name      TEXT,

            -- Timestamps
            created_at        TEXT,
            updated_at        TEXT
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_contra_date ON contra_vouchers(date)')

    # CREDIT NOTES TABLE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS credit_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Voucher identity
            credit_note_number TEXT UNIQUE,
            voucher_type      TEXT,
            date              TEXT,

            -- Account details
            from_account      TEXT,
            to_account        TEXT,
            amount            REAL DEFAULT 0,
            narration         TEXT,

            -- Ledger entries JSON (snapshot of all entries)
            ledger_entries    TEXT,

            -- Line items detail stored as JSON array.
            -- Each element: { item_name, quantity, rate, amount }
            line_items        TEXT,

            -- Cost center allocations JSON
            cost_center_allocations TEXT,

            -- Tally ID mapping
            tally_guid        TEXT,
            company_name      TEXT,

            -- Timestamps
            created_at        TEXT,
            updated_at        TEXT
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_credit_note_date ON credit_notes(date)')

    # DEBIT NOTES TABLE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS debit_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Voucher identity
            debit_note_number TEXT UNIQUE,
            voucher_type      TEXT,
            date              TEXT,

            -- Account details
            from_account      TEXT,
            to_account        TEXT,
            amount            REAL DEFAULT 0,
            narration         TEXT,

            -- Ledger entries JSON (snapshot of all entries)
            ledger_entries    TEXT,

            -- Line items detail stored as JSON array.
            -- Each element: { item_name, quantity, rate, amount }
            line_items        TEXT,

            -- Cost center allocations JSON
            cost_center_allocations TEXT,

            -- Tally ID mapping
            tally_guid        TEXT,
            company_name      TEXT,

            -- Timestamps
            created_at        TEXT,
            updated_at        TEXT
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_debit_note_date ON debit_notes(date)')

    # ZOHO TOKENS TABLE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS zoho_tokens (
            id INTEGER PRIMARY KEY DEFAULT 1,
            access_token TEXT,
            refresh_token TEXT,
            expiry_time TEXT,
            organization_id TEXT
        )
    ''')

    conn.commit()
    conn.close()
    print(f" Database initialized: {get_active_db()}")

# ---------------------------------------------------
# INSERTS / UPDATES
# ---------------------------------------------------

def insert_or_update_group(data):
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO groups (name, parent, primary_group)
            VALUES (:name, :parent, :primary_group)
            ON CONFLICT(name) DO UPDATE SET
                parent=excluded.parent,
                primary_group=excluded.primary_group
        ''', data)
        conn.commit()
    except Exception as e:
        print(f"Error saving group {data.get('name')}: {e}")


def bulk_save_contra(contra_data):
    if not contra_data:
        return
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO contra_vouchers (
            contra_number, voucher_type, date, from_account, to_account, amount,
            narration, ledger_entries, cost_center_allocations, tally_guid, company_name, created_at, updated_at
        ) VALUES (
            :contra_number, :voucher_type, :date, :from_account, :to_account, :amount,
            :narration, :ledger_entries, :cost_center_allocations, :tally_guid, :company_name, :created_at, :updated_at
        ) ON CONFLICT(contra_number) DO UPDATE SET
            date = excluded.date,
            from_account = excluded.from_account,
            to_account = excluded.to_account,
            amount = excluded.amount,
            narration = excluded.narration,
            ledger_entries = excluded.ledger_entries,
            cost_center_allocations = excluded.cost_center_allocations,
            updated_at = excluded.updated_at
    ''', contra_data)
    conn.commit()

def get_all_contra():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            SELECT * FROM contra_vouchers 
            ORDER BY date DESC, contra_number DESC
        ''')
        return cursor.fetchall()
    except Exception as e:
        print(f" Error getting all contra vouchers from DB: {e}")
        return []

def get_contra_by_number(contra_number):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM contra_vouchers WHERE contra_number = ?', (contra_number,))
        return cursor.fetchone()
    except Exception as e:
        print(f" Error getting contra {contra_number} from DB: {e}")
        return None


def insert_or_update_ledger(data):
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO ledgers (
                name, parent, type, address, state, country, pincode, email, phone,
                gstin, gst_reg_type, pan, opening_balance, closing_balance
            ) VALUES (
                :name, :parent, :type, :address, :state, :country, :pincode, :email, :phone,
                :gstin, :gst_reg_type, :pan, :opening_balance, :closing_balance
            )
            ON CONFLICT(name) DO UPDATE SET
                parent=excluded.parent,
                type=excluded.type,
                address=excluded.address,
                state=excluded.state,
                country=excluded.country,
                pincode=excluded.pincode,
                email=excluded.email,
                phone=excluded.phone,
                gstin=excluded.gstin,
                gst_reg_type=excluded.gst_reg_type,
                pan=excluded.pan,
                opening_balance=excluded.opening_balance,
                closing_balance=excluded.closing_balance
        ''', data)
        conn.commit()
    except Exception as e:
        print(f"Error saving ledger {data.get('name')}: {e}")


def insert_or_update_item(data):
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    
    try:
        # Check if category column exists, if not add it (Migration for existing DB)
        try:
            cursor.execute("ALTER TABLE items ADD COLUMN category TEXT")
        except sqlite3.OperationalError:
            pass # Column already exists

        cursor.execute('''
            INSERT INTO items (
                name, group_name, category, unit, hsn_source, hsn, description,
                gst_applicable, gst_rate_source, gst_rate, taxability, supply_type, rate_of_duty,
                qty, qty_unit, rate, rate_unit, value
            ) VALUES (
                :name, :group_name, :category, :unit, :hsn_source, :hsn, :description,
                :gst_applicable, :gst_rate_source, :gst_rate, :taxability, :supply_type, :rate_of_duty,
                :qty, :qty_unit, :rate, :rate_unit, :value
            )
            ON CONFLICT(name) DO UPDATE SET
                group_name=excluded.group_name,
                category=excluded.category,
                unit=excluded.unit,
                hsn_source=excluded.hsn_source,
                hsn=excluded.hsn,
                description=excluded.description,
                gst_applicable=excluded.gst_applicable,
                gst_rate_source=excluded.gst_rate_source,
                gst_rate=excluded.gst_rate,
                taxability=excluded.taxability,
                supply_type=excluded.supply_type,
                rate_of_duty=excluded.rate_of_duty,
                qty=excluded.qty,
                qty_unit=excluded.qty_unit,
                rate=excluded.rate,
                rate_unit=excluded.rate_unit,
                value=excluded.value
        ''', data)
        conn.commit()
    except Exception as e:
        print(f"Error saving item {data.get('name')}: {e}")


# ---------------------------------------------------
# GETTERS
# ---------------------------------------------------

def get_all_ledgers():
    conn = get_db_connection()
    ledgers = conn.execute('SELECT * FROM ledgers').fetchall()
    conn.close()
    return [dict(ix) for ix in ledgers]

def get_all_items():
    conn = get_db_connection()
    items = conn.execute('SELECT * FROM items').fetchall()
    conn.close()
    return [dict(ix) for ix in items]

def upsert_zoho_item_sync(item_name: str, zoho_item_id: str, signature: str = ""):
    if not item_name or not zoho_item_id:
        return
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO zoho_item_sync (item_name, zoho_item_id, signature, last_synced_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(item_name) DO UPDATE SET
                zoho_item_id=excluded.zoho_item_id,
                signature=excluded.signature,
                last_synced_at=excluded.last_synced_at
        ''', (item_name, zoho_item_id, signature or ""))
        conn.commit()
    except Exception as e:
        print(f"Error saving zoho item sync for {item_name}: {e}")

def get_zoho_item_sync_map():
    """
    Returns: dict lower(item_name) -> {"item_name": str, "zoho_item_id": str, "signature": str}
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    out = {}
    try:
        rows = cursor.execute('SELECT item_name, zoho_item_id, signature FROM zoho_item_sync').fetchall()
        for r in rows:
            nm = (r[0] or "").strip()
            if not nm:
                continue
            out[nm.lower()] = {
                "item_name": nm,
                "zoho_item_id": (r[1] or "").strip(),
                "signature": (r[2] or "").strip(),
            }
    except Exception:
        pass
    finally:
        conn.close()
    return out


def get_inv_adj_applied_ids(run_id: str):
    if not run_id:
        return set()
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        rows = cursor.execute(
            "SELECT item_id FROM inv_adj_applied WHERE run_id = ?",
            (run_id,)
        ).fetchall()
        return {str(r[0]) for r in rows if r and r[0]}
    except Exception:
        return set()
    finally:
        conn.close()


def mark_inv_adj_applied(run_id: str, item_id: str, item_name: str = ""):
    if not run_id or not item_id:
        return
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            INSERT INTO inv_adj_applied (run_id, item_id, item_name, applied_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(run_id, item_id) DO UPDATE SET
                item_name=excluded.item_name,
                applied_at=excluded.applied_at
            ''',
            (run_id, item_id, item_name or "")
        )
        conn.commit()
    except Exception as e:
        print(f"Error marking inv adj applied for {item_id}: {e}")


def get_ledger_by_name(name):
    conn = get_db_connection()
    ledger = conn.execute('SELECT * FROM ledgers WHERE name = ?', (name,)).fetchone()
    conn.close()
    return dict(ledger) if ledger else None

def get_all_groups():
    conn = get_db_connection()
    groups = conn.execute('SELECT * FROM groups').fetchall()
    conn.close()
    return [dict(ix) for ix in groups]

def get_all_cost_categories():
    conn = get_db_connection()
    valid = []
    try:
        rows = conn.execute('SELECT * FROM cost_categories').fetchall()
        valid = [dict(ix) for ix in rows]
    except:
        pass
    conn.close()
    return valid

def get_all_cost_centres():
    conn = get_db_connection()
    valid = []
    try:
        rows = conn.execute('SELECT * FROM cost_centres').fetchall()
        valid = [dict(ix) for ix in rows]
    except:
        pass
    conn.close()
    return valid

# ---------------------------------------------------
# COST CENTER FUNCTIONS
# ---------------------------------------------------

def insert_or_update_cost_category(data):
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO cost_categories (name, allocate_revenue, allocate_non_revenue)
            VALUES (:name, :allocate_revenue, :allocate_non_revenue)
            ON CONFLICT(name) DO UPDATE SET
                allocate_revenue=excluded.allocate_revenue,
                allocate_non_revenue=excluded.allocate_non_revenue
        ''', data)
        conn.commit()
    except Exception as e:
        print(f"Error saving cost category {data.get('name')}: {e}")


def insert_or_update_cost_centre(data):
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO cost_centres (name, category, parent)
            VALUES (:name, :category, :parent)
            ON CONFLICT(name) DO UPDATE SET
                category=excluded.category,
                parent=excluded.parent
        ''', data)
        conn.commit()
    except Exception as e:
        print(f"Error saving cost centre {data.get('name')}: {e}")


# ---------------------------------------------------
# RECEIPTS FUNCTIONS
# ---------------------------------------------------

def insert_or_update_receipt(data):
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO receipts (
                receipt_number, voucher_type, date,
                customer_name, customer_ledger_amount,
                payment_mode, bank_account, account_current_balance,
                amount, reference_number, against_reference,
                narration,
                invoice_allocations, ledger_entries, cost_center_allocations,
                rounding_amount, rounding_ledger,
                tally_guid, company_name,
                created_at, updated_at
            ) VALUES (
                :receipt_number, :voucher_type, :date,
                :customer_name, :customer_ledger_amount,
                :payment_mode, :bank_account, :account_current_balance,
                :amount, :reference_number, :against_reference,
                :narration,
                :invoice_allocations, :ledger_entries, :cost_center_allocations,
                :rounding_amount, :rounding_ledger,
                :tally_guid, :company_name,
                :created_at, :updated_at
            )
            ON CONFLICT(receipt_number) DO UPDATE SET
                voucher_type=excluded.voucher_type,
                date=excluded.date,
                customer_name=excluded.customer_name,
                customer_ledger_amount=excluded.customer_ledger_amount,
                payment_mode=excluded.payment_mode,
                bank_account=excluded.bank_account,
                account_current_balance=excluded.account_current_balance,
                amount=excluded.amount,
                reference_number=excluded.reference_number,
                against_reference=excluded.against_reference,
                narration=excluded.narration,
                invoice_allocations=excluded.invoice_allocations,
                ledger_entries=excluded.ledger_entries,
                cost_center_allocations=excluded.cost_center_allocations,
                rounding_amount=excluded.rounding_amount,
                rounding_ledger=excluded.rounding_ledger,
                tally_guid=excluded.tally_guid,
                company_name=excluded.company_name,
                updated_at=excluded.updated_at
        ''', data)
        conn.commit()
    except Exception as e:
        print(f"Error saving receipt {data.get('receipt_number')}: {e}")


def bulk_save_receipts(receipts_data):
    """
    Save multiple receipts using a SINGLE write connection.
    This is SQLite-safe and prevents 'database is locked' errors.
    """
    if not receipts_data:
        return

    # IMPORTANT: always use the global WRITE connection
    conn = get_db_connection(write=True)
    cursor = conn.cursor()

    cursor.executemany(
        '''
        INSERT INTO receipts (
            receipt_number,
            voucher_type,
            date,
            customer_name,
            customer_ledger_amount,
            payment_mode,
            bank_account,
            account_current_balance,
            amount,
            reference_number,
            against_reference,
            narration,
            invoice_allocations,
            ledger_entries,
            cost_center_allocations,
            rounding_amount,
            rounding_ledger,
            tally_guid,
            company_name,
            created_at,
            updated_at
        ) VALUES (
            :receipt_number,
            :voucher_type,
            :date,
            :customer_name,
            :customer_ledger_amount,
            :payment_mode,
            :bank_account,
            :account_current_balance,
            :amount,
            :reference_number,
            :against_reference,
            :narration,
            :invoice_allocations,
            :ledger_entries,
            :cost_center_allocations,
            :rounding_amount,
            :rounding_ledger,
            :tally_guid,
            :company_name,
            :created_at,
            :updated_at
        )
        ON CONFLICT(receipt_number) DO UPDATE SET
            voucher_type = excluded.voucher_type,
            date = excluded.date,
            customer_name = excluded.customer_name,
            customer_ledger_amount = excluded.customer_ledger_amount,
            payment_mode = excluded.payment_mode,
            bank_account = excluded.bank_account,
            account_current_balance = excluded.account_current_balance,
            amount = excluded.amount,
            reference_number = excluded.reference_number,
            against_reference = excluded.against_reference,
            narration = excluded.narration,
            invoice_allocations = excluded.invoice_allocations,
            ledger_entries = excluded.ledger_entries,
            cost_center_allocations = excluded.cost_center_allocations,
            rounding_amount = excluded.rounding_amount,
            rounding_ledger = excluded.rounding_ledger,
            tally_guid = excluded.tally_guid,
            company_name = excluded.company_name,
            updated_at = excluded.updated_at
        ''',
        receipts_data
    )
    conn.commit()


def bulk_save_receipts_by_guid(receipts_data):
    """
    Upsert receipts using tally_guid when available (preferred),
    otherwise falls back to receipt_number-based upsert.
    """
    if not receipts_data:
        return

    with_guid = []
    without_guid = []

    for r in receipts_data:
        guid = str((r or {}).get("tally_guid") or "").strip()
        if guid:
            with_guid.append(r)
        else:
            without_guid.append(r)

    conn = get_db_connection(write=True)
    cursor = conn.cursor()

    if with_guid:
        try:
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_receipts_tally_guid ON receipts(tally_guid) WHERE tally_guid != ''")
        except Exception:
            pass

        cursor.executemany(
            '''
            INSERT INTO receipts (
                receipt_number,
                voucher_type,
                date,
                customer_name,
                customer_ledger_amount,
                payment_mode,
                bank_account,
                account_current_balance,
                amount,
                reference_number,
                against_reference,
                narration,
                invoice_allocations,
                ledger_entries,
                cost_center_allocations,
                rounding_amount,
                rounding_ledger,
                tally_guid,
                company_name,
                created_at,
                updated_at
            ) VALUES (
                :receipt_number,
                :voucher_type,
                :date,
                :customer_name,
                :customer_ledger_amount,
                :payment_mode,
                :bank_account,
                :account_current_balance,
                :amount,
                :reference_number,
                :against_reference,
                :narration,
                :invoice_allocations,
                :ledger_entries,
                :cost_center_allocations,
                :rounding_amount,
                :rounding_ledger,
                :tally_guid,
                :company_name,
                :created_at,
                :updated_at
            )
            ON CONFLICT(tally_guid) DO UPDATE SET
                receipt_number = excluded.receipt_number,
                voucher_type = excluded.voucher_type,
                date = excluded.date,
                customer_name = excluded.customer_name,
                customer_ledger_amount = excluded.customer_ledger_amount,
                payment_mode = excluded.payment_mode,
                bank_account = excluded.bank_account,
                account_current_balance = excluded.account_current_balance,
                amount = excluded.amount,
                reference_number = excluded.reference_number,
                against_reference = excluded.against_reference,
                narration = excluded.narration,
                invoice_allocations = excluded.invoice_allocations,
                ledger_entries = excluded.ledger_entries,
                cost_center_allocations = excluded.cost_center_allocations,
                rounding_amount = excluded.rounding_amount,
                rounding_ledger = excluded.rounding_ledger,
                company_name = excluded.company_name,
                updated_at = excluded.updated_at
            ''',
            with_guid
        )

    if without_guid:
        cursor.executemany(
            '''
            INSERT INTO receipts (
                receipt_number,
                voucher_type,
                date,
                customer_name,
                customer_ledger_amount,
                payment_mode,
                bank_account,
                account_current_balance,
                amount,
                reference_number,
                against_reference,
                narration,
                invoice_allocations,
                ledger_entries,
                cost_center_allocations,
                rounding_amount,
                rounding_ledger,
                tally_guid,
                company_name,
                created_at,
                updated_at
            ) VALUES (
                :receipt_number,
                :voucher_type,
                :date,
                :customer_name,
                :customer_ledger_amount,
                :payment_mode,
                :bank_account,
                :account_current_balance,
                :amount,
                :reference_number,
                :against_reference,
                :narration,
                :invoice_allocations,
                :ledger_entries,
                :cost_center_allocations,
                :rounding_amount,
                :rounding_ledger,
                :tally_guid,
                :company_name,
                :created_at,
                :updated_at
            )
            ON CONFLICT(receipt_number) DO UPDATE SET
                voucher_type = excluded.voucher_type,
                date = excluded.date,
                customer_name = excluded.customer_name,
                customer_ledger_amount = excluded.customer_ledger_amount,
                payment_mode = excluded.payment_mode,
                bank_account = excluded.bank_account,
                account_current_balance = excluded.account_current_balance,
                amount = excluded.amount,
                reference_number = excluded.reference_number,
                against_reference = excluded.against_reference,
                narration = excluded.narration,
                invoice_allocations = excluded.invoice_allocations,
                ledger_entries = excluded.ledger_entries,
                cost_center_allocations = excluded.cost_center_allocations,
                rounding_amount = excluded.rounding_amount,
                rounding_ledger = excluded.rounding_ledger,
                tally_guid = excluded.tally_guid,
                company_name = excluded.company_name,
                updated_at = excluded.updated_at
            ''',
            without_guid
        )

    conn.commit()


def get_all_receipts():
    conn = get_db_connection()
    receipts = conn.execute('SELECT * FROM receipts ORDER BY date DESC').fetchall()
    conn.close()
    return [dict(ix) for ix in receipts]

def get_receipt_by_number(receipt_number):
    conn = get_db_connection()
    receipt = conn.execute('SELECT * FROM receipts WHERE receipt_number = ?', (receipt_number,)).fetchone()
    conn.close()
    return dict(receipt) if receipt else None


# ---------------------------------------------------
# JOURNALS FUNCTIONS
# ---------------------------------------------------

def insert_or_update_journal(data):
    """
    Upsert a single journal voucher.
    data keys: journal_number, date, narration, total_debit, total_credit,
               line_items (JSON string), from_date, to_date, created_at, updated_at
    """
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO journals (
                journal_number, date, narration,
                total_debit, total_credit,
                line_items,
                from_date, to_date,
                created_at, updated_at
            ) VALUES (
                :journal_number, :date, :narration,
                :total_debit, :total_credit,
                :line_items,
                :from_date, :to_date,
                :created_at, :updated_at
            )
            ON CONFLICT(journal_number) DO UPDATE SET
                date         = excluded.date,
                narration    = excluded.narration,
                total_debit  = excluded.total_debit,
                total_credit = excluded.total_credit,
                line_items   = excluded.line_items,
                from_date    = excluded.from_date,
                to_date      = excluded.to_date,
                updated_at   = excluded.updated_at
        ''', data)
        conn.commit()
    except Exception as e:
        print(f"Error saving journal {data.get('journal_number')}: {e}")


def bulk_save_journals(journals_data):
    """
    Save multiple journal vouchers using a SINGLE write connection.
    Same pattern as bulk_save_receipts() — prevents 'database is locked' errors.
    journals_data: list of dicts with the same keys as insert_or_update_journal.
    """
    if not journals_data:
        return

    conn = get_db_connection(write=True)
    cursor = conn.cursor()

    cursor.executemany(
        '''
        INSERT INTO journals (
            journal_number, date, narration,
            total_debit, total_credit,
            line_items,
            from_date, to_date,
            created_at, updated_at
        ) VALUES (
            :journal_number, :date, :narration,
            :total_debit, :total_credit,
            :line_items,
            :from_date, :to_date,
            :created_at, :updated_at
        )
        ON CONFLICT(journal_number) DO UPDATE SET
            date         = excluded.date,
            narration    = excluded.narration,
            total_debit  = excluded.total_debit,
            total_credit = excluded.total_credit,
            line_items   = excluded.line_items,
            from_date    = excluded.from_date,
            to_date      = excluded.to_date,
            updated_at   = excluded.updated_at
        ''',
        journals_data
    )
    conn.commit()
    print(f"    bulk_save_journals: saved {len(journals_data)} journals to DB")


def get_all_journals():
    """Return all journal vouchers ordered by date descending."""
    conn = get_db_connection()
    rows = conn.execute('SELECT * FROM journals ORDER BY date DESC').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_journal_by_number(journal_number):
    """Return a single journal voucher by its journal_number."""
    conn = get_db_connection()
    row = conn.execute(
        'SELECT * FROM journals WHERE journal_number = ?', (journal_number,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_journals_by_date_range(from_date, to_date, limit=None):
    """Return journals for a specific date range (YYYYMMDD strings)."""
    conn = get_db_connection()
    rows = conn.execute(
        'SELECT * FROM journals WHERE date >= ? AND date <= ? ORDER BY date DESC',
        (from_date, to_date)
    ).fetchall()
    conn.close()
    result = [dict(r) for r in rows]
    if limit:
        result = result[:limit]
    return result

# INVOICES FUNCTIONS
# ---------------------------------------------------

def bulk_save_invoices(invoices_data):
    """
    Save multiple invoice vouchers using a SINGLE write connection.
    Same pattern as bulk_save_receipts() — prevents 'database is locked' errors.
    invoices_data: list of dicts with all invoice fields.
    All complex fields (buyer_address, line_items, taxes) must be JSON strings.
    """
    if not invoices_data:
        return

    conn = get_db_connection(write=True)
    cursor = conn.cursor()

    cursor.executemany(
        '''
        INSERT INTO invoices (
            invoice_number, date, customer_name,
            po_number, buyer_address, payment_terms,
            sales_ledger, narration,
            irn, irn_ack_no, irn_ack_date,
            line_items, taxes,
            rounding_off, subtotal, tax_total, total_amount,
            from_date, to_date,
            created_at, updated_at
        ) VALUES (
            :invoice_number, :date, :customer_name,
            :po_number, :buyer_address, :payment_terms,
            :sales_ledger, :narration,
            :irn, :irn_ack_no, :irn_ack_date,
            :line_items, :taxes,
            :rounding_off, :subtotal, :tax_total, :total_amount,
            :from_date, :to_date,
            :created_at, :updated_at
        )
        ON CONFLICT(invoice_number) DO UPDATE SET
            date          = excluded.date,
            customer_name = excluded.customer_name,
            po_number     = excluded.po_number,
            buyer_address = excluded.buyer_address,
            payment_terms = excluded.payment_terms,
            sales_ledger  = excluded.sales_ledger,
            narration     = excluded.narration,
            irn           = excluded.irn,
            irn_ack_no    = excluded.irn_ack_no,
            irn_ack_date  = excluded.irn_ack_date,
            line_items    = excluded.line_items,
            taxes         = excluded.taxes,
            rounding_off  = excluded.rounding_off,
            subtotal      = excluded.subtotal,
            tax_total     = excluded.tax_total,
            total_amount  = excluded.total_amount,
            from_date     = excluded.from_date,
            to_date       = excluded.to_date,
            updated_at    = excluded.updated_at
        ''',
        invoices_data
    )
    conn.commit()
    print(f"    bulk_save_invoices: saved {len(invoices_data)} invoices to DB")


def get_all_invoices():
    """Return all invoice vouchers ordered by date descending."""
    conn = get_db_connection()
    rows = conn.execute('SELECT * FROM invoices ORDER BY date DESC').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_invoice_by_number(invoice_number):
    """Return a single invoice by its invoice_number."""
    conn = get_db_connection()
    row = conn.execute(
        'SELECT * FROM invoices WHERE invoice_number = ?', (invoice_number,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_invoices_by_date_range(from_date, to_date):
    """Return invoices for a specific date range (YYYYMMDD strings)."""
    conn = get_db_connection()
    rows = conn.execute(
        'SELECT * FROM invoices WHERE date >= ? AND date <= ? ORDER BY date DESC',
        (from_date, to_date)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------
# BILLS FUNCTIONS
# ---------------------------------------------------

def bulk_save_bills(bills_data):
    """
    Save multiple Purchase bill vouchers using a SINGLE write connection.
    Same pattern as bulk_save_receipts() — prevents 'database is locked' errors.
    bills_data: list of dicts with all bill fields.
    All complex fields (vendor_address, line_items, taxes) must be JSON strings.
    """
    if not bills_data:
        return

    conn = get_db_connection(write=True)
    cursor = conn.cursor()

    cursor.executemany(
        '''
        INSERT INTO bills (
            bill_number, date, vendor_name,
            po_number, reference_number, vendor_address,
            payment_terms, purchase_ledger, narration,
            line_items, taxes,
            rounding_off, subtotal, tax_total, total_amount,
            from_date, to_date,
            created_at, updated_at
        ) VALUES (
            :bill_number, :date, :vendor_name,
            :po_number, :reference_number, :vendor_address,
            :payment_terms, :purchase_ledger, :narration,
            :line_items, :taxes,
            :rounding_off, :subtotal, :tax_total, :total_amount,
            :from_date, :to_date,
            :created_at, :updated_at
        )
        ON CONFLICT(bill_number) DO UPDATE SET
            date             = excluded.date,
            vendor_name      = excluded.vendor_name,
            po_number        = excluded.po_number,
            reference_number = excluded.reference_number,
            vendor_address   = excluded.vendor_address,
            payment_terms    = excluded.payment_terms,
            purchase_ledger  = excluded.purchase_ledger,
            narration        = excluded.narration,
            line_items       = excluded.line_items,
            taxes            = excluded.taxes,
            rounding_off     = excluded.rounding_off,
            subtotal         = excluded.subtotal,
            tax_total        = excluded.tax_total,
            total_amount     = excluded.total_amount,
            from_date        = excluded.from_date,
            to_date          = excluded.to_date,
            updated_at       = excluded.updated_at
        ''',
        bills_data
    )
    conn.commit()
    print(f"    bulk_save_bills: saved {len(bills_data)} bills to DB")


def get_all_bills():
    """Return all bill vouchers ordered by date descending."""
    conn = get_db_connection()
    rows = conn.execute('SELECT * FROM bills ORDER BY date DESC').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_bill_by_number(bill_number):
    """Return a single bill by its bill_number."""
    conn = get_db_connection()
    row = conn.execute(
        'SELECT * FROM bills WHERE bill_number = ?', (bill_number,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_bills_by_date_range(from_date, to_date):
    """Return bills for a specific date range (YYYYMMDD strings)."""
    conn = get_db_connection()
    rows = conn.execute(
        'SELECT * FROM bills WHERE date >= ? AND date <= ? ORDER BY date DESC',
        (from_date, to_date)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------
# SALES ORDERS FUNCTIONS
# ---------------------------------------------------

def bulk_save_sales_orders(orders_data):
    """
    Save multiple Sales Order vouchers using a SINGLE write connection.
    Prevents 'database is locked' errors.
    All complex fields (customer_address, line_items, taxes) must be JSON strings.
    """
    if not orders_data:
        return

    conn = get_db_connection(write=True)
    cursor = conn.cursor()

    cursor.executemany(
        '''
        INSERT INTO sales_orders (
            sales_order_number, date, customer_name,
            reference_number, customer_address,
            payment_terms, order_status, sales_ledger, narration,
            line_items, taxes,
            rounding_off, subtotal, tax_total, total_amount,
            from_date, to_date,
            created_at, updated_at
        ) VALUES (
            :sales_order_number, :date, :customer_name,
            :reference_number, :customer_address,
            :payment_terms, :order_status, :sales_ledger, :narration,
            :line_items, :taxes,
            :rounding_off, :subtotal, :tax_total, :total_amount,
            :from_date, :to_date,
            :created_at, :updated_at
        )
        ON CONFLICT(sales_order_number) DO UPDATE SET
            date             = excluded.date,
            customer_name    = excluded.customer_name,
            reference_number = excluded.reference_number,
            customer_address = excluded.customer_address,
            payment_terms    = excluded.payment_terms,
            order_status     = excluded.order_status,
            sales_ledger     = excluded.sales_ledger,
            narration        = excluded.narration,
            line_items       = excluded.line_items,
            taxes            = excluded.taxes,
            rounding_off     = excluded.rounding_off,
            subtotal         = excluded.subtotal,
            tax_total        = excluded.tax_total,
            total_amount     = excluded.total_amount,
            from_date        = excluded.from_date,
            to_date          = excluded.to_date,
            updated_at       = excluded.updated_at
        ''',
        orders_data
    )
    conn.commit()
    print(f"    bulk_save_sales_orders: saved {len(orders_data)} sales orders to DB")


def get_all_sales_orders():
    """Return all Sales Orders ordered by date descending."""
    conn = get_db_connection()
    rows = conn.execute('SELECT * FROM sales_orders ORDER BY date DESC').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_sales_order_by_number(sales_order_number):
    """Return a single Sales Order by its sales_order_number."""
    conn = get_db_connection()
    row = conn.execute(
        'SELECT * FROM sales_orders WHERE sales_order_number = ?', (sales_order_number,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_sales_orders_by_date_range(from_date, to_date):
    """Return Sales Orders for a specific date range (YYYYMMDD strings)."""
    conn = get_db_connection()
    rows = conn.execute(
        'SELECT * FROM sales_orders WHERE date >= ? AND date <= ? ORDER BY date DESC',
        (from_date, to_date)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------
# PURCHASE ORDERS FUNCTIONS
# ---------------------------------------------------

def bulk_save_purchase_orders(orders_data):
    """
    Save multiple Purchase Order vouchers using a SINGLE write connection.
    Prevents 'database is locked' errors.
    All complex fields (vendor_address, line_items, taxes) must be JSON strings.
    """
    if not orders_data:
        return

    conn = get_db_connection(write=True)
    cursor = conn.cursor()

    cursor.executemany(
        '''
        INSERT INTO purchase_orders (
            purchase_order_number, date, vendor_name,
            reference_number, vendor_address,
            payment_terms, order_status, purchase_ledger, narration,
            line_items, taxes,
            rounding_off, subtotal, tax_total, total_amount,
            from_date, to_date,
            created_at, updated_at
        ) VALUES (
            :purchase_order_number, :date, :vendor_name,
            :reference_number, :vendor_address,
            :payment_terms, :order_status, :purchase_ledger, :narration,
            :line_items, :taxes,
            :rounding_off, :subtotal, :tax_total, :total_amount,
            :from_date, :to_date,
            :created_at, :updated_at
        )
        ON CONFLICT(purchase_order_number) DO UPDATE SET
            date                  = excluded.date,
            vendor_name           = excluded.vendor_name,
            reference_number      = excluded.reference_number,
            vendor_address        = excluded.vendor_address,
            payment_terms         = excluded.payment_terms,
            order_status          = excluded.order_status,
            purchase_ledger       = excluded.purchase_ledger,
            narration             = excluded.narration,
            line_items            = excluded.line_items,
            taxes                 = excluded.taxes,
            rounding_off          = excluded.rounding_off,
            subtotal              = excluded.subtotal,
            tax_total             = excluded.tax_total,
            total_amount          = excluded.total_amount,
            from_date             = excluded.from_date,
            to_date               = excluded.to_date,
            updated_at            = excluded.updated_at
        ''',
        orders_data
    )
    conn.commit()
    print(f"    bulk_save_purchase_orders: saved {len(orders_data)} purchase orders to DB")


def get_all_purchase_orders():
    """Return all Purchase Orders ordered by date descending."""
    conn = get_db_connection()
    rows = conn.execute('SELECT * FROM purchase_orders ORDER BY date DESC').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_purchase_order_by_number(purchase_order_number):
    """Return a single Purchase Order by its purchase_order_number."""
    conn = get_db_connection()
    row = conn.execute(
        'SELECT * FROM purchase_orders WHERE purchase_order_number = ?', (purchase_order_number,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_purchase_orders_by_date_range(from_date, to_date):
    """Return Purchase Orders for a specific date range (YYYYMMDD strings)."""
    conn = get_db_connection()
    rows = conn.execute(
        'SELECT * FROM purchase_orders WHERE date >= ? AND date <= ? ORDER BY date DESC',
        (from_date, to_date)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ---------------------------------------------------
# PAYMENTS MADE
# ---------------------------------------------------

def bulk_save_payments_made(payments_data):
    if not payments_data:
        return
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO payments_made (
            payment_number, voucher_type, date, vendor_name, vendor_ledger_amount,
            payment_mode, bank_account, account_current_balance, amount,
            reference_number, against_reference, narration,
            bill_allocations, ledger_entries, cost_center_allocations,
            rounding_amount, rounding_ledger, tally_guid, company_name,
            created_at, updated_at
        ) VALUES (
            :payment_number, :voucher_type, :date, :vendor_name, :vendor_ledger_amount,
            :payment_mode, :bank_account, :account_current_balance, :amount,
            :reference_number, :against_reference, :narration,
            :bill_allocations, :ledger_entries, :cost_center_allocations,
            :rounding_amount, :rounding_ledger, :tally_guid, :company_name,
            :created_at, :updated_at
        ) ON CONFLICT(payment_number) DO UPDATE SET
            date = excluded.date,
            vendor_name = excluded.vendor_name,
            vendor_ledger_amount = excluded.vendor_ledger_amount,
            payment_mode = excluded.payment_mode,
            bank_account = excluded.bank_account,
            account_current_balance = excluded.account_current_balance,
            amount = excluded.amount,
            reference_number = excluded.reference_number,
            against_reference = excluded.against_reference,
            narration = excluded.narration,
            bill_allocations = excluded.bill_allocations,
            ledger_entries = excluded.ledger_entries,
            cost_center_allocations = excluded.cost_center_allocations,
            rounding_amount = excluded.rounding_amount,
            rounding_ledger = excluded.rounding_ledger,
            updated_at = excluded.updated_at
    ''', payments_data)
    conn.commit()

def get_all_payments_made():
    conn = get_db_connection()
    rows = conn.execute('SELECT * FROM payments_made ORDER BY date DESC').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_expense_ledgers():
    """Identify expense ledgers more reliably by checking both type and parent group keywords."""
    conn = get_db_connection()
    # Check for 'other' type or parents containing 'Expense' or 'Purchase'
    query = """
        SELECT name FROM ledgers 
        WHERE LOWER(type) IN ('other', 'others')
           OR LOWER(parent) LIKE '%expense%'
           OR LOWER(parent) LIKE '%purchase%'
           OR LOWER(parent) LIKE '%cost of sales%'
    """
    rows = conn.execute(query).fetchall()
    conn.close()
    return [r['name'].strip().lower() for r in rows if r['name']]

# ---------------------------------------------------
# CREDIT NOTES FUNCTIONS
# ---------------------------------------------------

def bulk_save_credit_notes(credit_notes_data):
    if not credit_notes_data:
        return
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO credit_notes (
            credit_note_number, voucher_type, date, from_account, to_account, amount,
            narration, ledger_entries, line_items, cost_center_allocations, tally_guid, company_name, created_at, updated_at
        ) VALUES (
            :credit_note_number, :voucher_type, :date, :from_account, :to_account, :amount,
            :narration, :ledger_entries, :line_items, :cost_center_allocations, :tally_guid, :company_name, :created_at, :updated_at
        ) ON CONFLICT(credit_note_number) DO UPDATE SET
            date = excluded.date,
            from_account = excluded.from_account,
            to_account = excluded.to_account,
            amount = excluded.amount,
            narration = excluded.narration,
            ledger_entries = excluded.ledger_entries,
            line_items = excluded.line_items,
            cost_center_allocations = excluded.cost_center_allocations,
            updated_at = excluded.updated_at
    ''', credit_notes_data)
    conn.commit()

def get_all_credit_notes():
    conn = get_db_connection()
    rows = conn.execute('SELECT * FROM credit_notes ORDER BY date DESC').fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ---------------------------------------------------
# DEBIT NOTES FUNCTIONS
# ---------------------------------------------------

def bulk_save_debit_notes(debit_notes_data):
    if not debit_notes_data:
        return
    conn = get_db_connection(write=True)
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO debit_notes (
            debit_note_number, voucher_type, date, from_account, to_account, amount,
            narration, ledger_entries, line_items, cost_center_allocations, tally_guid, company_name, created_at, updated_at
        ) VALUES (
            :debit_note_number, :voucher_type, :date, :from_account, :to_account, :amount,
            :narration, :ledger_entries, :line_items, :cost_center_allocations, :tally_guid, :company_name, :created_at, :updated_at
        ) ON CONFLICT(debit_note_number) DO UPDATE SET
            date = excluded.date,
            from_account = excluded.from_account,
            to_account = excluded.to_account,
            amount = excluded.amount,
            narration = excluded.narration,
            ledger_entries = excluded.ledger_entries,
            line_items = excluded.line_items,
            cost_center_allocations = excluded.cost_center_allocations,
            updated_at = excluded.updated_at
    ''', debit_notes_data)
    conn.commit()

def get_all_debit_notes():
    conn = get_db_connection()
    rows = conn.execute('SELECT * FROM debit_notes ORDER BY date DESC').fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_credit_note_by_number(credit_note_number):
    conn = get_db_connection()
    row = conn.execute('SELECT * FROM credit_notes WHERE credit_note_number = ?', (credit_note_number,)).fetchone()
    conn.close()
    return dict(row) if row else None

def get_debit_note_by_number(debit_note_number):
    conn = get_db_connection()
    row = conn.execute('SELECT * FROM debit_notes WHERE debit_note_number = ?', (debit_note_number,)).fetchone()
    conn.close()
    return dict(row) if row else None

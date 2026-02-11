import sqlite3
import os

DB_NAME = "tally_data.db"

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # GROUPS TABLE
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            parent TEXT,
            primary_group TEXT
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
    
    conn.commit()
    conn.close()
    print(f"✅ Database initialized: {DB_NAME}")

# ---------------------------------------------------
# INSERTS / UPDATES
# ---------------------------------------------------

def insert_or_update_group(data):
    conn = get_db_connection()
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
    finally:
        conn.close()

def insert_or_update_ledger(data):
    conn = get_db_connection()
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
    finally:
        conn.close()

def insert_or_update_item(data):
    conn = get_db_connection()
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
    finally:
        conn.close()

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
    conn = get_db_connection()
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
    finally:
        conn.close()

def insert_or_update_cost_centre(data):
    conn = get_db_connection()
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
    finally:
        conn.close()

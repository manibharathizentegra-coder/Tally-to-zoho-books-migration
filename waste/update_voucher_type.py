import sqlite3

def run_db_update():
    DB_NAME = "tally_data.db"
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Find the expense ledgers
    cursor.execute("SELECT name FROM ledgers WHERE LOWER(type) = 'other' OR LOWER(type) = 'others'")
    rows = cursor.fetchall()
    expense_ledgers = [row[0].strip().lower() for row in rows if row[0]]

    if not expense_ledgers:
        print("No expense ledgers found to update.")
        conn.close()
        return

    # Update the payments_made table's voucher_type column
    # Use IN clause for efficiency
    placeholders = ', '.join(['?'] * len(expense_ledgers))
    sql = f"UPDATE payments_made SET voucher_type = 'Expense' WHERE LOWER(vendor_name) IN ({placeholders})"
    
    cursor.execute(sql, expense_ledgers)
    count = cursor.rowcount
    
    conn.commit()
    conn.close()
    print(f" Successfully updated {count} vouchers in payments_made to 'Expense'.")

run_db_update()

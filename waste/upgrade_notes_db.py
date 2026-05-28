import sqlite3

def upgrade_tables():
    DB_NAME = "tally_data.db"
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Add line_items to credit_notes if not exists
    try:
        cursor.execute("ALTER TABLE credit_notes ADD COLUMN line_items TEXT")
        print(" Added line_items column to credit_notes")
    except sqlite3.OperationalError:
        print("️ line_items column already exists in credit_notes")

    # Add line_items to debit_notes if not exists
    try:
        cursor.execute("ALTER TABLE debit_notes ADD COLUMN line_items TEXT")
        print(" Added line_items column to debit_notes")
    except sqlite3.OperationalError:
        print("️ line_items column already exists in debit_notes")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    upgrade_tables()

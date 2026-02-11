import sqlite3
import os

DB_NAME = "tally_data.db"

def verify_db():
    if not os.path.exists(DB_NAME):
        print(f"❌ Database file '{DB_NAME}' not found. Run the sync scripts first.")
        return

    try:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        tables = ["groups", "ledgers", "items"]
        
        print(f"🔍 Verifying Database: {DB_NAME}\n")
        
        for table in tables:
            print(f"📋 TABLE: {table.upper()}")
            print("-" * 40)
            
            # Count
            try:
                count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"   Row Count: {count}")
            except sqlite3.OperationalError:
                print(f"   ❌ Table '{table}' does not exist.")
                continue

            # Sample Data
            if count > 0:
                print("   Sample Data (First 3 rows):")
                rows = cursor.execute(f"SELECT * FROM {table} LIMIT 3").fetchall()
                for row in rows:
                    print(f"   - {dict(row)}")
            else:
                print("   (Table is empty)")
            
            print("\n")
            
        conn.close()
        
    except Exception as e:
        print(f"❌ Error validating DB: {e}")

if __name__ == "__main__":
    verify_db()

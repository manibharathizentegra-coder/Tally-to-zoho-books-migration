import re

def update_split_function():
    with open('app.py', 'r', encoding='utf-8') as f:
        content = f.read()

    new_split_code = """
@app.route('/api/payments/split_expenses', methods=['POST'])
def split_payments_expenses():
    if not database_manager:
        from flask import jsonify
        return jsonify({"status": "error", "error": "Database manager not available"}), 500
        
    try:
        from flask import request, jsonify
        data = request.json or {}
        payments = data.get('payments', [])
        
        conn = database_manager.get_db_connection()
        cursor = conn.cursor()
        
        # 1. First, identify expense ledgers
        cursor.execute("SELECT name FROM ledgers WHERE LOWER(type) = 'other' OR LOWER(type) = 'others'")
        expense_ledgers_list = [row['name'].strip().lower() for row in cursor.fetchall() if row['name']]
        
        if expense_ledgers_list:
            # 2. Update the database permanently
            placeholders = ', '.join(['?'] * len(expense_ledgers_list))
            update_sql = f"UPDATE payments_made SET voucher_type = 'Expense' WHERE LOWER(vendor_name) IN ({placeholders})"
            cursor.execute(update_sql, expense_ledgers_list)
            conn.commit()
            updated_count = cursor.rowcount
            print(f" DB Update: {updated_count} rows changed to 'Expense'")

        # 3. Categorize current data for frontend return
        expense_ledgers_set = set(expense_ledgers_list)
        vendor_payments = []
        expenses = []
        
        for p in payments:
            vendor = str(p.get("vendor_name", "")).strip().lower()
            if vendor in expense_ledgers_set:
                p['voucher_type'] = 'Expense' # Update local object too
                expenses.append(p)
            else:
                vendor_payments.append(p)
                
        conn.close()
        return jsonify({
            "status": "success",
            "vendor_payments": vendor_payments,
            "expenses": expenses,
            "message": f"Successfully split {len(expenses)} expenses and updated database."
        })
    except Exception as e:
        import traceback
        from flask import jsonify
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(e)}), 500
"""
    # Regex find the old split_payments_expenses function block
    # Start: `@app.route('/api/payments/split_expenses', methods=['POST'])`
    # End: `return jsonify({ ... })` (last one before next function or `if __name__ == '__main__':`)
    
    start_match = "@app.route('/api/payments/split_expenses', methods=['POST'])"
    if start_match in content:
        # Since I know I just injected it at the end earlier, I'll search from that point.
        start_idx = content.find(start_match)
        end_idx = content.find("if __name__ == '__main__':", start_idx)
        if end_idx == -1:
            end_idx = len(content)
            
        modified_content = content[:start_idx] + new_split_code.strip() + "\n\n" + content[end_idx:]
        
        with open('app.py', 'w', encoding='utf-8') as f:
            f.write(modified_content)
        print("Updated split function with database UPDATE logic in app.py")
    else:
        print("Error: Could not find split function to update.")

update_split_function()

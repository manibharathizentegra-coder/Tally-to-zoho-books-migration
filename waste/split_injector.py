import re

def inject_route():
    with open('app.py', 'r', encoding='utf-8') as f:
        content = f.read()

    split_route = """
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
        
        # Get expense ledgers (type 'Others' or 'other')
        cursor.execute("SELECT name FROM ledgers WHERE LOWER(type) = 'other' OR LOWER(type) = 'others'")
        expense_ledgers = [row['name'].strip().lower() for row in cursor.fetchall()]
        conn.close()
        
        vendor_payments = []
        expenses = []
        
        for p in payments:
            vendor = str(p.get("vendor_name", "")).strip().lower()
            if vendor in expense_ledgers:
                expenses.append(p)
            else:
                vendor_payments.append(p)
                
        return jsonify({
            "status": "success",
            "vendor_payments": vendor_payments,
            "expenses": expenses,
            "message": f"Successfully split {len(expenses)} expenses from {len(payments)} total payments."
        })
    except Exception as e:
        import traceback
        from flask import jsonify
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(e)}), 500
"""

    if "def split_payments_expenses()" not in content:
        # Insert it above `if __name__ == '__main__':`
        pos = content.rfind("if __name__ == '__main__':")
        if pos != -1:
            content = content[:pos] + split_route + "\n\n" + content[pos:]
        else:
            content += "\n" + split_route
            
        with open('app.py', 'w', encoding='utf-8') as f:
            f.write(content)
        print("Injected split API route into app.py")
    else:
        print("Split API route already exists in app.py")

inject_route()

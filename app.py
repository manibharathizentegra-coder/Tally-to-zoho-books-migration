from flask import Flask, jsonify, render_template, send_file, request
from flask_cors import CORS
import sys
import os
import json

# Add modules directory to path
sys.path.append(os.path.dirname(__file__))

# Import backend modules
try:
    from ledgers import ledgers_backend as ledgers_module
    print("✅ Successfully imported ledgers_text backend")
except ImportError as e:
    print(f"❌ Error importing ledgers_backend: {e}")
    ledgers_module = None

try:
    from items import items_backend as items_module
    print("✅ Successfully imported items_backend")
except ImportError as e:
    print(f"❌ Error importing items_backend: {e}")
    items_module = None

try:
    from journel import journel_backend as journel_module
    print("✅ Successfully imported journel_backend")
except ImportError as e:
    print(f"❌ Error importing journel_backend: {e}")
    journel_module = None

try:
    from invoice import invoice_backend as invoice_module
    print("✅ Successfully imported invoice_backend")
except ImportError as e:
    print(f"❌ Error importing invoice_backend: {e}")
    invoice_module = None

try:
    from bills import bills_backend as bills_module
    print("✅ Successfully imported bills_backend")
except ImportError as e:
    print(f"❌ Error importing bills_backend: {e}")
    bills_module = None

try:
    from sales_order import sale_backend as sales_order_module
    print("✅ Successfully imported sales_order_backend")
except ImportError as e:
    print(f"❌ Error importing sales_order_backend: {e}")
    sales_order_module = None

try:
    from purchase_order import purchase_order_backend as purchase_order_module
    print("✅ Successfully imported purchase_order_backend")
except ImportError as e:
    print(f"❌ Error importing purchase_order_backend: {e}")
    purchase_order_module = None

try:
    from receipts import receipts_backend as receipts_module
    print("✅ Successfully imported receipts_backend")
except ImportError as e:
    print(f"❌ Error importing receipts_backend: {e}")
    receipts_module = None

try:
    import database_manager
    print("✅ Successfully imported database_manager")
except ImportError as e:
    print(f"❌ Error importing database_manager: {e}")
    database_manager = None

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------
# ROUTES
# ---------------------------------------------------------

@app.route('/api/db/ledgers', methods=['GET'])
def api_db_ledgers():
    if not database_manager: return jsonify({"error": "DB Manager not loaded"}), 500
    try:
        ledgers = database_manager.get_all_ledgers()
        return jsonify({"ledgers": ledgers, "count": len(ledgers)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/db/items', methods=['GET'])
def api_db_items():
    if not database_manager: return jsonify({"error": "DB Manager not loaded"}), 500
    try:
        items = database_manager.get_all_items()
        return jsonify({"items": items, "count": len(items)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/db/groups', methods=['GET'])
def api_db_groups():
    if not database_manager: return jsonify({"error": "DB Manager not loaded"}), 500
    try:
        groups = database_manager.get_all_groups()
        return jsonify({"groups": groups, "count": len(groups)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/db/cost-categories', methods=['GET'])
def api_db_cost_categories():
    if not database_manager: return jsonify({"error": "DB Manager not loaded"}), 500
    try:
        data = database_manager.get_all_cost_categories()
        return jsonify({"categories": data, "count": len(data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/db/cost-centres', methods=['GET'])
def api_db_cost_centres():
    if not database_manager: return jsonify({"error": "DB Manager not loaded"}), 500
    try:
        data = database_manager.get_all_cost_centres()
        return jsonify({"centres": data, "count": len(data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/cost-centers/fetch', methods=['GET'])
def api_fetch_cost_centers():
    try:
        from cost_centers import cost_center_backend
        data = cost_center_backend.get_all_cost_data()
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/cost-centers/sync-reporting-tags', methods=['POST'])
def api_sync_reporting_tags():
    try:
        from cost_centers import cost_center_backend
        result = cost_center_backend.sync_reporting_tags_to_zoho()
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/')
def index():
    return render_template('ledgers.html')

@app.route('/ledgers')
def ledgers_page():
    return render_template('ledgers.html')

@app.route('/items')
def items_page():
    return render_template('items.html')

# ---------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------

@app.route('/api/ledgers/fetch', methods=['GET'])
def api_fetch_ledgers():
    try:
        data = ledgers_module.analyze_ledgers_and_groups()
        if data:
            return jsonify(data)
        return jsonify({"error": "Failed to fetch data from Tally"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/items/fetch', methods=['GET'])
def api_fetch_items():
    try:
        data = items_module.get_all_items_data()
        if data:
            return jsonify(data)
        return jsonify({"error": "Failed to fetch items from Tally"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/ledgers/sync_zoho', methods=['POST'])
def api_sync_ledgers():
    try:
        selected = request.json.get("ledgers") if request.is_json else None
        result = ledgers_module.sync_ledgers_to_zoho(selected)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/ledgers/sync_customers', methods=['POST'])
def api_sync_customers():
    """Sync ONLY customers to Zoho Books."""
    try:
        selected = request.json.get("ledgers") if request.is_json else None
        result = ledgers_module.sync_ledgers_to_zoho(selected, contact_type_filter='customer')
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/ledgers/sync_vendors', methods=['POST'])
def api_sync_vendors():
    """Sync ONLY vendors to Zoho Books."""
    try:
        selected = request.json.get("ledgers") if request.is_json else None
        result = ledgers_module.sync_ledgers_to_zoho(selected, contact_type_filter='vendor')
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/ledgers/save_mapping', methods=['POST'])
def api_save_group_mapping():
    try:
        mapping = request.json.get("mapping") if request.is_json else {}
        ledgers_module.save_groups_mapping(mapping)
        return jsonify({"status": "success", "message": "Mapping saved successfully"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/ledgers/get_mapping', methods=['GET'])
def api_get_group_mapping():
    try:
        mapping = ledgers_module.get_groups_mapping()
        return jsonify(mapping)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/ledgers/execute_group_sync', methods=['POST'])
def api_execute_group_sync():
    try:
        # Load mapping from file in backend
        result = ledgers_module.sync_groups_to_zoho(None)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/ledgers/create_standalone', methods=['POST'])
def api_create_standalone():
    try:
        ledger_name = request.json.get("ledger_name") if request.is_json else None
        account_type = request.json.get("account_type") if request.is_json else None
        if not ledger_name or not account_type:
            return jsonify({"status": "error", "message": "ledger_name and account_type are required"}), 400
        result = ledgers_module.create_standalone_account(ledger_name, account_type)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/items/sync_zoho', methods=['POST'])
def api_sync_items():
    try:
        selected = request.json.get("items") if request.is_json else None
        result = items_module.sync_items_to_zoho(selected)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Journal routes
@app.route('/journals')
def journals_page():
    return render_template('journals.html')

@app.route('/api/journals/fetch', methods=['POST'])
def api_fetch_journals():
    try:
        # Get date range from request
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        
        data = journel_module.get_all_journals_data(from_date, to_date, limit)
        if data:
            return jsonify(data)
        return jsonify({"error": "Failed to fetch journals from Tally"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/journals/sync_zoho', methods=['POST'])
def api_sync_journals():
    try:
        selected = request.json.get("journals") if request.is_json else None
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        
        result = journel_module.sync_journals_to_zoho(selected, from_date, to_date, limit)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Invoice routes
@app.route('/invoices')
def invoices_page():
    return render_template('invoices.html')

@app.route('/api/invoices/fetch', methods=['POST'])
def api_fetch_invoices():
    try:
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        
        data = invoice_module.get_all_invoices_data(from_date, to_date, limit)
        if data:
            return jsonify(data)
        return jsonify({"error": "Failed to fetch invoices from Tally"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/invoices/sync_zoho', methods=['POST'])
def api_sync_invoices():
    try:
        selected = request.json.get("invoices") if request.is_json else None
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        
        result = invoice_module.sync_invoices_to_zoho(selected, from_date, to_date, limit)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Bills routes
@app.route('/bills')
def bills_page():
    return render_template('bills.html')

@app.route('/api/bills/fetch', methods=['POST'])
def api_fetch_bills():
    try:
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        
        data = bills_module.get_all_bills_data(from_date, to_date, limit)
        if data:
            return jsonify(data)
        return jsonify({"error": "Failed to fetch bills from Tally"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/bills/sync_zoho', methods=['POST'])
def api_sync_bills():
    try:
        selected = request.json.get("bills") if request.is_json else None
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        
        result = bills_module.sync_bills_to_zoho(selected, from_date, to_date, limit)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Sales Order routes
@app.route('/sales_orders')
def sales_orders_page():
    return render_template('sales_orders.html')

@app.route('/api/sales_orders/fetch', methods=['POST'])
def api_fetch_sales_orders():
    try:
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        
        data = sales_order_module.get_all_sales_orders_data(from_date, to_date, limit)
        if data:
            return jsonify(data)
        return jsonify({"error": "Failed to fetch sales orders from Tally"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/sales_orders/sync_zoho', methods=['POST'])
def api_sync_sales_orders():
    try:
        selected = request.json.get("sales_orders") if request.is_json else None
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        
        result = sales_order_module.sync_sales_orders_to_zoho(selected, from_date, to_date, limit)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Purchase Order routes
@app.route('/purchase_orders')
def purchase_orders_page():
    return render_template('purchase_orders.html')

@app.route('/api/purchase_orders/fetch', methods=['POST'])
def api_fetch_purchase_orders():
    try:
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        
        data = purchase_order_module.get_all_purchase_orders_data(from_date, to_date, limit)
        if data:
            return jsonify(data)
        return jsonify({"error": "Failed to fetch purchase orders from Tally"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/purchase_orders/sync_zoho', methods=['POST'])
def api_sync_purchase_orders():
    try:
        selected = request.json.get("purchase_orders") if request.is_json else None
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        
        result = purchase_order_module.sync_purchase_orders_to_zoho(selected, from_date, to_date, limit)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Receipts (Payment Received) routes
@app.route('/receipts')
def receipts_page():
    return render_template('receipts.html')

@app.route('/api/receipts/fetch', methods=['POST'])
def api_fetch_receipts():
    try:
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        company_name = request.json.get("company_name") if request.is_json else None
        
        data = receipts_module.get_all_receipts_data(from_date, to_date, limit, company_name)
        if data:
            return jsonify(data)
        return jsonify({"error": "Failed to fetch receipts from Tally"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/receipts/sync_zoho', methods=['POST'])
def api_sync_receipts():
    try:
        selected = request.json.get("receipts") if request.is_json else None
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        company_name = request.json.get("company_name") if request.is_json else None
        
        result = receipts_module.sync_receipts_to_zoho(selected, from_date, to_date, limit, company_name)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/db/receipts', methods=['GET'])
def api_db_receipts():
    """Fetch receipts from SQLite database"""
    try:
        receipts = database_manager.get_all_receipts()
        
        # Parse JSON fields back to lists/dicts
        for receipt in receipts:
            # Parse invoice_allocations
            if receipt.get('invoice_allocations'):
                try:
                    if isinstance(receipt['invoice_allocations'], str):
                        receipt['invoice_allocations'] = json.loads(receipt['invoice_allocations'])
                except Exception as e:
                    print(f"⚠️ Error parsing invoice_allocations for receipt {receipt.get('receipt_number')}: {e}")
                    receipt['invoice_allocations'] = []
            else:
                receipt['invoice_allocations'] = []
            
            # Parse ledger_entries
            if receipt.get('ledger_entries'):
                try:
                    if isinstance(receipt['ledger_entries'], str):
                        receipt['ledger_entries'] = json.loads(receipt['ledger_entries'])
                except Exception as e:
                    print(f"⚠️ Error parsing ledger_entries for receipt {receipt.get('receipt_number')}: {e}")
                    receipt['ledger_entries'] = []
            else:
                receipt['ledger_entries'] = []
            
            # Parse cost_center_allocations
            if receipt.get('cost_center_allocations'):
                try:
                    if isinstance(receipt['cost_center_allocations'], str):
                        receipt['cost_center_allocations'] = json.loads(receipt['cost_center_allocations'])
                except Exception as e:
                    print(f"⚠️ Error parsing cost_center_allocations for receipt {receipt.get('receipt_number')}: {e}")
                    receipt['cost_center_allocations'] = []
            else:
                receipt['cost_center_allocations'] = []
        
        # Calculate stats
        total_amount = sum(float(r.get('amount', 0) or 0) for r in receipts)
        
        return jsonify({
            "receipts": receipts,
            "count": len(receipts),
            "total_amount": total_amount
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/journals/refresh_cache', methods=['POST'])
def api_refresh_cache():
    try:
        refresh_type = request.json.get("type", "all") if request.is_json else "all"
        
        stats = {}
        
        # Refresh Tally data
        if refresh_type in ["all", "tally"]:
            ledger_map = journel_module.get_ledger_map_from_tally(use_cache=False, force_refresh=True)
            if ledger_map:
                vendors = sum(1 for t in ledger_map.values() if t == "vendor")
                customers = sum(1 for t in ledger_map.values() if t == "customer")
                accounts = sum(1 for t in ledger_map.values() if t == "account")
                stats["tally"] = {
                    "ledgers": len(ledger_map),
                    "vendors": vendors,
                    "customers": customers,
                    "others": accounts
                }
        
        # Refresh Zoho data
        if refresh_type in ["all", "zoho"]:
            token = journel_module.get_access_token()
            if token:
                # Refresh accounts (Chart of Accounts)
                account_map = journel_module.get_zoho_accounts(token, use_cache=False, force_refresh=True)
                # Refresh contacts
                contact_map = journel_module.get_zoho_contacts(token, use_cache=False, force_refresh=True)
                
                # Count contact types
                zoho_vendors = sum(1 for c in contact_map.values() if c["contact_type"] == "vendor")
                zoho_customers = sum(1 for c in contact_map.values() if c["contact_type"] == "customer")
                
                stats["zoho"] = {
                    "total_contacts": len(contact_map) if contact_map else 0,
                    "vendors": zoho_vendors,
                    "customers": zoho_customers,
                    "chart_of_accounts": len(account_map) if account_map else 0
                }
        
        return jsonify({
            "status": "success",
            "stats": stats
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    print("🚀 Starting Tally Software Frontend...")
    print("📍 URL: http://localhost:5000")
    app.run(debug=True, port=5000)

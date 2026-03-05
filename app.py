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
    import importlib.util
    import os
    spec = importlib.util.spec_from_file_location("payments_backend", os.path.join(os.path.dirname(__file__), 'Payments made', 'payments_backend.py'))
    payments_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(payments_module)
    print("✅ Successfully imported payments_backend")
except Exception as e:
    print(f"❌ Error importing payments_backend: {e}")
    payments_module = None

try:
    spec_contra = importlib.util.spec_from_file_location("contra_backend", os.path.join(os.path.dirname(__file__), 'contra', 'contra_backend.py'))
    contra_module = importlib.util.module_from_spec(spec_contra)
    spec_contra.loader.exec_module(contra_module)
    print("✅ Successfully imported contra_backend")
except Exception as e:
    print(f"❌ Error importing contra_backend: {e}")
    contra_module = None

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

# ---------------------------------------------------------
# OPENING BALANCE — Upload Tally file → Download Zoho Excel
# ---------------------------------------------------------

@app.route('/opening-balance')
def opening_balance_page():
    return render_template('opening_balance.html')

@app.route('/api/opening-balance/convert', methods=['POST'])
def api_convert_opening_balance():
    """
    Single-file upload: tally_file
    ?preview=1  → JSON summary + preview rows
    ?preview=0  → Excel file download
    """
    try:
        from opening_balance_converter import convert
        import io

        tally_f = request.files.get('tally_file')
        if not tally_f or not tally_f.filename:
            return jsonify({"error": "Tally file is required"}), 400

        tally_bytes    = tally_f.read()
        migration_date = request.form.get('migration_date', None)
        preview_only   = request.args.get('preview', '1') == '1'

        # Fetch existing Groups from DB to filter out Group Headers
        db_group_names = []
        if database_manager:
            try:
                # get_all_groups returns list of dicts: [{'name': 'X'}, ...]
                all_groups = database_manager.get_all_groups()
                db_group_names = [g['name'] for g in all_groups if g.get('name')]
            except Exception as e:
                print(f"Warning: Failed to fetch groups for filtering: {e}")

        output_bytes, summary, errors = convert(
            tally_bytes, tally_f.filename, migration_date, db_group_names
        )

        if output_bytes is None:
            return jsonify({"error": errors[0] if errors else "Conversion failed"}), 400

        if preview_only:
            return jsonify({"status": "ok", "summary": summary, "errors": errors})
        else:
            return send_file(
                io.BytesIO(output_bytes),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name='zoho_opening_balance.xlsx'
            )

    except Exception as e:
        import traceback
        traceback.print_exc()
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

# Payments Made routes
@app.route('/payments_made')
def payments_made_page():
    return render_template('payments_made.html')

@app.route('/api/payments_made/fetch', methods=['POST'])
def api_fetch_payments_made():
    try:
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        company_name = request.json.get("company_name") if request.is_json else None
        
        data = payments_module.get_all_payments_data(from_date, to_date, limit, company_name)
        if data:
            return jsonify(data)
        return jsonify({"error": "Failed to fetch payments made from Tally"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/payments_made/upload', methods=['POST'])
def api_upload_payments_made():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400
        
        import tempfile
        import os
        from datetime import datetime
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as temp:
            file.save(temp.name)
            temp_path = temp.name
            
        parsed_payments = payments_module.parse_tally_json(temp_path)
        
        # Save to SQLite
        if database_manager and parsed_payments:
            database_manager.init_db()
            db_data_list = []
            for payment in parsed_payments:
                db_data = {
                    "payment_number": payment.get("payment_number", ""),
                    "voucher_type": payment.get("voucher_type", ""),
                    "date": payment.get("date", ""),
                    "vendor_name": payment.get("vendor_name", ""),
                    "vendor_ledger_amount": payment.get("vendor_ledger_amount", 0) or 0,
                    "payment_mode": payment.get("payment_mode", ""),
                    "bank_account": payment.get("bank_account", ""),
                    "account_current_balance": payment.get("account_current_balance", 0) or 0,
                    "amount": payment.get("amount", 0) or 0,
                    "reference_number": payment.get("reference_number", ""),
                    "against_reference": payment.get("against_reference", ""),
                    "narration": payment.get("narration", ""),
                    "bill_allocations": json.dumps(payment.get("bill_allocations", [])),
                    "ledger_entries": json.dumps(payment.get("ledger_entries", [])),
                    "cost_center_allocations": json.dumps(payment.get("cost_center_allocations", [])),
                    "rounding_amount": payment.get("rounding_amount", 0) or 0,
                    "rounding_ledger": payment.get("rounding_ledger", ""),
                    "tally_guid": payment.get("tally_guid", ""),
                    "company_name": "",
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat()
                }
                db_data_list.append(db_data)
                
            try:
                database_manager.bulk_save_payments_made(db_data_list)
            except AttributeError:
                pass
                
        os.unlink(temp_path)
        
        total_amount = sum(float(p.get("amount", 0)) for p in parsed_payments)
        return jsonify({"payments": parsed_payments, "total_amount": total_amount})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/payments_made/sync_zoho', methods=['POST'])
def api_sync_payments_made():
    try:
        selected = request.json.get("payments_made") if request.is_json else None
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        company_name = request.json.get("company_name") if request.is_json else None
        
        result = payments_module.sync_payments_to_zoho(selected, from_date, to_date, limit, company_name)
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

@app.route('/api/db/sales_orders', methods=['GET'])
def api_db_sales_orders():
    """Fetch Sales Order vouchers stored in SQLite database (tally_data.db)"""
    if not database_manager:
        return jsonify({"error": "DB Manager not loaded"}), 500
    try:
        orders = database_manager.get_all_sales_orders()

        json_fields = ('customer_address', 'line_items', 'taxes')
        for so in orders:
            for field in json_fields:
                if so.get(field):
                    try:
                        if isinstance(so[field], str):
                            so[field] = json.loads(so[field])
                    except Exception:
                        so[field] = []
                else:
                    so[field] = []

        total_amount = sum(float(so.get('total_amount', 0) or 0) for so in orders)
        return jsonify({
            "sales_orders": orders,
            "count":        len(orders),
            "total_amount": round(total_amount, 2)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/db/purchase_orders', methods=['GET'])
def api_db_purchase_orders():
    """Fetch Purchase Order vouchers stored in SQLite database (tally_data.db)"""
    if not database_manager:
        return jsonify({"error": "DB Manager not loaded"}), 500
    try:
        orders = database_manager.get_all_purchase_orders()

        json_fields = ('vendor_address', 'line_items', 'taxes')
        for po in orders:
            for field in json_fields:
                if po.get(field):
                    try:
                        if isinstance(po[field], str):
                            po[field] = json.loads(po[field])
                    except Exception:
                        po[field] = []
                else:
                    po[field] = []

        total_amount = sum(float(po.get('total_amount', 0) or 0) for po in orders)
        return jsonify({
            "purchase_orders": orders,
            "count":           len(orders),
            "total_amount":    round(total_amount, 2)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/db/bills', methods=['GET'])
def api_db_bills():
    """Fetch bill vouchers stored in SQLite database (tally_data.db)"""
    if not database_manager:
        return jsonify({"error": "DB Manager not loaded"}), 500
    try:
        bills = database_manager.get_all_bills()

        # Parse JSON columns back to lists — same pattern as receipts
        json_fields = ('vendor_address', 'line_items', 'taxes')
        for bill in bills:
            for field in json_fields:
                if bill.get(field):
                    try:
                        if isinstance(bill[field], str):
                            bill[field] = json.loads(bill[field])
                    except Exception:
                        bill[field] = []
                else:
                    bill[field] = []

        total_amount = sum(float(bill.get('total_amount', 0) or 0) for bill in bills)

        return jsonify({
            "bills":        bills,
            "count":        len(bills),
            "total_amount": round(total_amount, 2)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/db/invoices', methods=['GET'])
def api_db_invoices():
    """Fetch invoice vouchers stored in SQLite database (tally_data.db)"""
    if not database_manager:
        return jsonify({"error": "DB Manager not loaded"}), 500
    try:
        invoices = database_manager.get_all_invoices()

        # Parse JSON columns back to lists — same pattern as receipts
        json_fields = ('buyer_address', 'line_items', 'taxes')
        for inv in invoices:
            for field in json_fields:
                if inv.get(field):
                    try:
                        if isinstance(inv[field], str):
                            inv[field] = json.loads(inv[field])
                    except Exception:
                        inv[field] = []
                else:
                    inv[field] = []

        total_amount = sum(float(inv.get('total_amount', 0) or 0) for inv in invoices)

        return jsonify({
            "invoices":     invoices,
            "count":        len(invoices),
            "total_amount": round(total_amount, 2)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/db/journals', methods=['GET'])
def api_db_journals():
    """Fetch journal vouchers stored in SQLite database (tally_data.db)"""
    if not database_manager:
        return jsonify({"error": "DB Manager not loaded"}), 500
    try:
        journals = database_manager.get_all_journals()

        # Parse line_items JSON back to list — same as receipts does for its JSON columns
        for j in journals:
            if j.get('line_items'):
                try:
                    if isinstance(j['line_items'], str):
                        j['line_items'] = json.loads(j['line_items'])
                except Exception:
                    j['line_items'] = []
            else:
                j['line_items'] = []

        total_debit  = sum(float(j.get('total_debit',  0) or 0) for j in journals)
        total_credit = sum(float(j.get('total_credit', 0) or 0) for j in journals)

        return jsonify({
            "journals":     journals,
            "count":        len(journals),
            "total_debit":  round(total_debit,  2),
            "total_credit": round(total_credit, 2)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/db/payments_made', methods=['GET'])
def api_db_payments_made():
    """Fetch payments made from SQLite database"""
    try:
        payments = database_manager.get_all_payments_made()
        
        for payment in payments:
            if payment.get('bill_allocations'):
                try:
                    if isinstance(payment['bill_allocations'], str):
                        payment['bill_allocations'] = json.loads(payment['bill_allocations'])
                except Exception as e:
                    print(f"⚠️ Error parsing bill_allocations for payment {payment.get('payment_number')}: {e}")
                    payment['bill_allocations'] = []
            else:
                payment['bill_allocations'] = []
            
            if payment.get('ledger_entries'):
                try:
                    if isinstance(payment['ledger_entries'], str):
                        payment['ledger_entries'] = json.loads(payment['ledger_entries'])
                except Exception as e:
                    print(f"⚠️ Error parsing ledger_entries for payment {payment.get('payment_number')}: {e}")
                    payment['ledger_entries'] = []
            else:
                payment['ledger_entries'] = []
            
            if payment.get('cost_center_allocations'):
                try:
                    if isinstance(payment['cost_center_allocations'], str):
                        payment['cost_center_allocations'] = json.loads(payment['cost_center_allocations'])
                except Exception as e:
                    print(f"⚠️ Error parsing cost_center_allocations for payment {payment.get('payment_number')}: {e}")
                    payment['cost_center_allocations'] = []
            else:
                payment['cost_center_allocations'] = []
        
        total_amount = sum(float(p.get('amount', 0) or 0) for p in payments)
        
        return jsonify({
            "payments": payments,
            "count": len(payments),
            "total_amount": total_amount
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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

# ---------------------------------------------------------
# CONTRA ROUTES
# ---------------------------------------------------------
@app.route('/contra')
def contra_page():
    return render_template('contra.html')

@app.route('/api/contra/fetch', methods=['POST'])
def api_fetch_contra():
    try:
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        
        data = contra_module.get_all_contra_data(from_date, to_date, limit)
        if data:
            return jsonify(data)
        return jsonify({"error": "Failed to fetch contra from Tally"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/contra/upload', methods=['POST'])
def api_upload_contra():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400
        
        import tempfile, os
        from datetime import datetime
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as temp:
            file.save(temp.name)
            temp_path = temp.name
            
        parsed_contras = contra_module.parse_tally_json(temp_path)
        
        # Save to SQLite
        if database_manager and parsed_contras:
            database_manager.init_db()
            db_data_list = []
            for contra in parsed_contras:
                db_data = {
                    "contra_number": contra.get("contra_number", ""),
                    "voucher_type": contra.get("voucher_type", "Contra"),
                    "date": contra.get("date", ""),
                    "from_account": contra.get("from_account", ""),
                    "to_account": contra.get("to_account", ""),
                    "amount": contra.get("amount", 0) or 0,
                    "narration": contra.get("narration", ""),
                    "ledger_entries": json.dumps(contra.get("ledger_entries", [])),
                    "cost_center_allocations": json.dumps(contra.get("cost_center_allocations", [])),
                    "tally_guid": contra.get("tally_guid", ""),
                    "company_name": "",
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat()
                }
                db_data_list.append(db_data)
                
            try:
                database_manager.bulk_save_contra(db_data_list)
            except AttributeError:
                pass
                
        os.unlink(temp_path)
        
        total_amount = sum(float(c.get("amount", 0)) for c in parsed_contras)
        return jsonify({"contra_vouchers": parsed_contras, "total_amount": total_amount})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/contra/sync_zoho', methods=['POST'])
def api_sync_contra():
    try:
        selected = request.json.get("contras") if request.is_json else None
        from_date = request.json.get("from_date", "20250401") if request.is_json else "20250401"
        to_date = request.json.get("to_date", "20250430") if request.is_json else "20250430"
        limit = request.json.get("limit") if request.is_json else None
        
        result = contra_module.sync_contra_to_zoho(selected, from_date, to_date, limit)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/db/contra', methods=['GET'])
def api_db_contra():
    if not database_manager: return jsonify({"error": "DB Manager not loaded"}), 500
    try:
        c_rows = database_manager.get_all_contra()
        contras = []
        for r in c_rows:
            d = dict(r)
            if isinstance(d.get('ledger_entries'), str):
                try: d['ledger_entries'] = json.loads(d['ledger_entries'])
                except: d['ledger_entries'] = []
            if isinstance(d.get('cost_center_allocations'), str):
                try: d['cost_center_allocations'] = json.loads(d['cost_center_allocations'])
                except: d['cost_center_allocations'] = []
            contras.append(d)
                
        total_amount = sum(float(r.get('amount', 0) or 0) for r in contras)
        return jsonify({
            "contra_vouchers": contras,
            "count": len(contras),
            "total_amount": total_amount
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("🚀 Starting Tally Software Frontend...")
    print("📍 URL: http://localhost:5000")
    app.run(debug=True, port=5000)

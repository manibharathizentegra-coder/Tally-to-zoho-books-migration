from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import Software_With_Front_END.ledgers.ledgers_backend as tally
import os

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

# Cache for data
cached_data = None

@app.route('/')
def index():
    """Serve the main HTML page"""
    return send_file('ledgers.html')

@app.route('/api/fetch', methods=['GET'])
def fetch_ledgers():
    """Fetch fresh data from Tally"""
    global cached_data
    
    try:
        print("📡 Fetching data from Tally...")
        
        # Fetch groups and ledgers
        groups = tally.fetch_groups_from_tally()
        ledgers = tally.fetch_ledgers_from_tally()
        
        if not ledgers:
            return jsonify({
                "error": "Failed to fetch ledgers from Tally. Make sure Tally is running on port 9000."
            }), 500
        
        # Analyze and classify
        analysis = tally.analyze_ledgers(ledgers, groups)
        
        # Build response
        cached_data = {
            "ledgers": analysis["ledgers"],
            "sundry_debtors": analysis["sundry_debtors"],
            "sundry_creditors": analysis["sundry_creditors"],
            "other_ledgers": analysis["other_ledgers"],
            "stats": {
                "total": len(analysis["ledgers"]),
                "customers": len(analysis["sundry_debtors"]),
                "vendors": len(analysis["sundry_creditors"]),
                "others": len(analysis["other_ledgers"])
            }
        }
        
        print(f"✅ Data fetched: {cached_data['stats']['total']} ledgers")
        return jsonify(cached_data)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": f"Server error: {str(e)}"
        }), 500

@app.route('/api/ledgers', methods=['GET'])
def get_ledgers():
    """Get cached ledger data"""
    if cached_data is None:
        return jsonify({
            "error": "No data loaded. Please fetch data first."
        }), 404
    
    return jsonify(cached_data)

@app.route('/api/search', methods=['GET'])
def search_ledgers():
    """Search ledgers by query"""
    if cached_data is None:
        return jsonify({
            "error": "No data loaded. Please fetch data first."
        }), 404
    
    query = request.args.get('q', '').lower().strip()
    
    if not query:
        return jsonify(cached_data)
    
    # Search across all ledgers
    matches = [
        ledger for ledger in cached_data["ledgers"]
        if query in ledger["name"].lower()
    ]
    
    # Classify matched results
    customers = [l for l in matches if l["type"] == "customer"]
    vendors = [l for l in matches if l["type"] == "vendor"]
    others = [l for l in matches if l["type"] == "other"]
    
    return jsonify({
        "ledgers": matches,
        "sundry_debtors": customers,
        "sundry_creditors": vendors,
        "other_ledgers": others,
        "stats": {
            "total": len(matches),
            "customers": len(customers),
            "vendors": len(vendors),
            "others": len(others)
        }
    })

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get statistics"""
    if cached_data is None:
        return jsonify({
            "error": "No data loaded. Please fetch data first."
        }), 404
    
    return jsonify(cached_data["stats"])

if __name__ == '__main__':
    print("🚀 Starting Ledger Analysis API Server...")
    print("📍 Server running on http://localhost:5000")
    print("📄 Open http://localhost:5000 in your browser")
    print("\n⚠️  Make sure Tally is running on port 9000!")
    print("-" * 60)
    
    app.run(debug=True, port=5000, host='0.0.0.0')

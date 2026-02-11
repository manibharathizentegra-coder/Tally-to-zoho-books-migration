
# ----------------------------------------------------------
# API WRAPPER FOR FRONTEND
# ----------------------------------------------------------

def get_all_purchase_orders_data(from_date="20250401", to_date="20250430", limit=None):
    """
    Wrapper function for API to get purchase order data
    Returns formatted data for frontend display
    """
    try:
        purchase_orders = fetch_tally_purchase_orders_range(from_date, to_date, limit)
        
        if not purchase_orders:
            return None
        
        # Calculate stats
        total_orders = len(purchase_orders)
        total_amount = sum(po.get("total_amount", 0) for po in purchase_orders)
        
        return {
            "purchase_orders": purchase_orders,
            "stats": {
                "total_orders": total_orders,
                "total_amount": round(total_amount, 2),
                "from_date": from_date,
                "to_date": to_date
            }
        }
    except Exception as e:
        print(f"❌ Error in get_all_purchase_orders_data: {e}")
        import traceback
        traceback.print_exc()
        return None

def fetch_tally_purchase_orders_range(from_date="20250401", to_date="20250430", limit=None):
    """
    Fetch Purchase Orders from Tally with ALL fields
    
    Args:
        from_date: Start date in YYYYMMDD format
        to_date: End date in YYYYMMDD format
        limit: Maximum number of purchase orders to fetch
    """
    ledger_map = get_ledger_map_from_tally()
    
    xml_request = f"""<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
    <BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <VOUCHERTYPENAME>Purchase Order</VOUCHERTYPENAME>
    <SVFROMDATE>{from_date}</SVFROMDATE><SVTODATE>{to_date}</SVTODATE>
    </STATICVARIABLES></REQUESTDESC></EXPORTDATA></BODY></ENVELOPE>"""

    try:
        print(f"📥 Fetching purchase orders from Tally ({from_date} to {to_date})...")
        response = requests.post(TALLY_URL, data=xml_request, timeout=90)
        soup = BeautifulSoup(response.content, 'lxml-xml')
        
        vouchers = soup.find_all('VOUCHER')
        if limit:
            vouchers = vouchers[:limit]
        
        purchase_order_data = []
        
        for v in vouchers:
            v_date = v.find('DATE').text if v.find('DATE') else ""
            v_no = v.find('VOUCHERNUMBER').text if v.find('VOUCHERNUMBER') else ""
            vendor_name = v.find('PARTYNAME').text if v.find('PARTYNAME') else ""
            narration = v.find('NARRATION').text if v.find('NARRATION') else ""
            
            # Get Reference Number
            reference_number = v.find('REFERENCE').text if v.find('REFERENCE') else ""
            
            # Get Vendor Address
            vendor_address = []
            buyer_addr_list = v.find('BASICBUYERADDRESS.LIST')
            if buyer_addr_list:
                for addr in buyer_addr_list.find_all('BASICBUYERADDRESS'):
                    if addr.text:
                        vendor_address.append(addr.text.strip())
            
            # Get Payment Terms
            payment_terms = get_payment_terms_hierarchical(v, vendor_name)
            
            # Get Order Status
            order_status = v.find('ORDERSTATUS').text if v.find('ORDERSTATUS') else "Pending"
            
            # Get Purchase Ledger
            purchase_ledger = ""
            for item in v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST'):
                item_ledger = item.find('LEDGERNAME')
                if item_ledger and item_ledger.text:
                    purchase_ledger = item_ledger.text.strip()
                    break
            
            if not purchase_ledger:
                max_positive_amount = 0
                for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                    name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                    amount_tag = entry.find('AMOUNT')
                    if amount_tag and amount_tag.text:
                        numbers = re.findall(r'[-\d.]+', amount_tag.text)
                        amt = float(numbers[-1]) if numbers else 0.0
                    else:
                        amt = 0.0
                    
                    name_lower = name.lower()
                    if name == vendor_name or 'cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower or 'rounding' in name_lower:
                        continue
                    
                    if amt > max_positive_amount:
                        max_positive_amount = amt
                        purchase_ledger = name
            
            # Get line items
            line_items = []
            subtotal = 0
            
            for item in v.find_all('INVENTORYENTRIES.LIST') or v.find_all('ALLINVENTORYENTRIES.LIST'):
                item_name = item.find('STOCKITEMNAME').text.strip() if item.find('STOCKITEMNAME') else ""
                
                qty_tag = item.find('ACTUALQTY') or item.find('BILLEDQTY')
                quantity = qty_tag.text.strip() if qty_tag else "0"
                
                rate_tag = item.find('RATE')
                if rate_tag and rate_tag.text:
                    rate_text = rate_tag.text.split('/')[0].strip()
                    numbers = re.findall(r'[-\d.]+', rate_text)
                    rate = float(numbers[-1]) if numbers else 0.0
                else:
                    rate = 0.0
                
                discount_tag = item.find('DISCOUNT')
                discount = discount_tag.text.strip() if discount_tag else "0"
                
                amount_tag = item.find('AMOUNT')
                if amount_tag and amount_tag.text:
                    amount_text = amount_tag.text.strip()
                    numbers = re.findall(r'[-\d.]+', amount_text)
                    amount = float(numbers[-1]) if numbers else 0.0
                else:
                    amount = 0.0
                
                # Get reporting tags
                category = ""
                cost_centre = ""
                cat_alloc = item.find('CATEGORYALLOCATIONS.LIST')
                if cat_alloc:
                    category_tag = cat_alloc.find('CATEGORY')
                    if category_tag:
                        category = category_tag.text.strip()
                    cc_list = cat_alloc.find('COSTCENTREALLOCATIONS.LIST')
                    if cc_list:
                        cc_name = cc_list.find('NAME')
                        if cc_name:
                            cost_centre = cc_name.text.strip()
                
                line_items.append({
                    "item_name": item_name,
                    "quantity": quantity,
                    "rate": rate,
                    "discount": discount,
                    "amount": abs(amount),
                    "category": category,
                    "cost_centre": cost_centre
                })
                
                subtotal += abs(amount)
            
            # Get tax details
            taxes = []
            tax_total = 0
            for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                
                amount_tag = entry.find('AMOUNT')
                if amount_tag and amount_tag.text:
                    amount_text = amount_tag.text.strip()
                    numbers = re.findall(r'[-\d.]+', amount_text)
                    amt = float(numbers[-1]) if numbers else 0.0
                else:
                    amt = 0.0
                
                name_lower = name.lower()
                if ('cgst' in name_lower or 'sgst' in name_lower or 'igst' in name_lower) and 'output' in name_lower:
                    tax_rate = ""
                    if '%' in name:
                        tax_rate = name.split('%')[0].split()[-1]
                    
                    tax_type = "CGST" if 'cgst' in name_lower else ("SGST" if 'sgst' in name_lower else "IGST")
                    taxes.append({
                        "tax_name": name,
                        "tax_type": tax_type,
                        "tax_rate": tax_rate,
                        "tax_amount": abs(amt)
                    })
                    tax_total += abs(amt)
            
            # Get rounding off
            rounding_off = 0.0
            for entry in v.find_all('LEDGERENTRIES.LIST') or v.find_all('ALLLEDGERENTRIES.LIST'):
                name = entry.find('LEDGERNAME').text.strip() if entry.find('LEDGERNAME') else ""
                if 'rounding' in name.lower():
                    amount_tag = entry.find('AMOUNT')
                    if amount_tag and amount_tag.text:
                        numbers = re.findall(r'[-\d.]+', amount_tag.text)
                        rounding_off = float(numbers[-1]) if numbers else 0.0
                    break
            
            total_amount = subtotal + tax_total + rounding_off
            
            purchase_order_data.append({
                "purchase_order_number": v_no,
                "date": v_date,
                "vendor_name": vendor_name,
                "reference_number": reference_number,
                "vendor_address": vendor_address,
                "payment_terms": payment_terms,
                "order_status": order_status,
                "purchase_ledger": purchase_ledger,
                "narration": narration,
                "line_items": line_items,
                "taxes": taxes,
                "rounding_off": rounding_off,
                "subtotal": round(subtotal, 2),
                "tax_total": round(tax_total, 2),
                "total_amount": round(total_amount, 2)
            })
        
        print(f"✅ Fetched {len(purchase_order_data)} purchase order(s)")
        return purchase_order_data
    
    except Exception as e:
        print(f"❌ Error fetching Tally purchase orders: {e}")
        import traceback
        traceback.print_exc()
        return []

def sync_purchase_orders_to_zoho(selected_orders=None, from_date="20250401", to_date="20250430", limit=None):
    """
    Sync purchase orders to Zoho Books
    
    Args:
        selected_orders: List of purchase order objects to sync (if None, fetches from Tally)
        from_date: Start date in YYYYMMDD format
        to_date: End date in YYYYMMDD format
        limit: Maximum number of purchase orders to sync
    """
    try:
        print("🚀 Starting Zoho Sync (Purchase Orders)...")
        
        # Get access token
        token = get_access_token()
        if not token:
            return {"status": "error", "message": "Failed to get access token"}
        
        # Get Zoho data
        contact_map = get_zoho_contacts(token)
        account_map = get_zoho_accounts(token)
        payment_terms_map = get_zoho_payment_terms_list(token)
        tax_map = get_zoho_taxes(token)
        tag_map = get_zoho_tags(token)
        item_map = get_zoho_items(token)
        
        # Get purchase orders to sync
        if not selected_orders:
            orders_to_sync = fetch_tally_purchase_orders_range(from_date, to_date, limit)
        else:
            orders_to_sync = selected_orders
            if limit and len(orders_to_sync) > limit:
                orders_to_sync = orders_to_sync[:limit]
        
        if not orders_to_sync:
            return {"status": "error", "message": "No purchase orders to sync"}
        
        print(f"📊 Syncing {len(orders_to_sync)} purchase order(s) to Zoho Books...")
        
        stats = {"created": 0, "failed": 0, "errors": []}
        
        for po in orders_to_sync:
            result = create_zoho_purchase_order(token, po, contact_map, account_map, payment_terms_map, tax_map, tag_map, item_map)
            if result.get("success"):
                stats["created"] += 1
                print(f"✅ Synced Purchase Order #{po['purchase_order_number']}")
            else:
                stats["failed"] += 1
                stats["errors"].append({
                    "purchase_order_number": po['purchase_order_number'],
                    "vendor": po['vendor_name'],
                    "error": result.get("error", "Unknown error")
                })
                print(f"❌ Failed Purchase Order #{po['purchase_order_number']}")
        
        return {"status": "success", "stats": stats}
        
    except Exception as e:
        print(f"❌ Error in sync_purchase_orders_to_zoho: {e}")
        return {"status": "error", "message": str(e)}

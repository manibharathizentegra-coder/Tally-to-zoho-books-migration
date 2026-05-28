import os

def replace_in_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Carefully replace credit note references to debit note
    content = content.replace('credit_note_backend', 'debit_note_backend')
    content = content.replace('credit_note_number', 'debit_note_number')
    content = content.replace('credit_notes', 'debit_notes')
    content = content.replace('credit_note_data', 'debit_note_data')
    content = content.replace('credit_note_date', 'debit_note_date')
    content = content.replace('creditNoteData', 'debitNoteData')
    
    # Capitalized / Title representations
    content = content.replace('Credit Note details', 'Debit Note details')
    content = content.replace('Credit Note Details', 'Debit Note Details')
    content = content.replace('Credit Note Vouchers', 'Debit Note Vouchers')
    content = content.replace('Credit Notes', 'Debit Notes')
    content = content.replace('Credit Note', 'Debit Note')
    
    # API endpoints and smaller things
    content = content.replace('/api/credit_note/', '/api/debit_note/')
    content = content.replace('/api/db/credit_notes', '/api/db/debit_notes')
    content = content.replace('href="/credit_note"', 'href="/debit_note"')
    
    # Python definitions and variables
    content = content.replace('get_all_credit_note_data', 'get_all_debit_note_data')
    content = content.replace('fetch_tally_credit_note', 'fetch_tally_debit_note')
    content = content.replace('sync_credit_note_to_zoho', 'sync_debit_note_to_zoho')
    content = content.replace('_fetch_day_credit_note', '_fetch_day_debit_note')
    
    # JS/HTML
    content = content.replace('allCreditNotes', 'allDebitNotes')
    content = content.replace('total-credit-notes', 'total-debit-notes')
    content = content.replace('renderCreditNotes', 'renderDebitNotes')
    content = content.replace('creditNoteModal', 'debitNoteModal')
    content = content.replace('creditNoteDetails', 'debitNoteDetails')
    content = content.replace('credit_note:', 'debit_note:')
    content = content.replace('credit_note.html', 'debit_note.html')
    
    # Any other lower cases
    content = content.replace('credit_note', 'debit_note')
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

replace_in_file('debit_note/debit_note_backend.py')
replace_in_file('templates/debit_note.html')
print("Debit Note replacements done.")

import os
import re

def replace_in_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # We want to be careful.
    # contra_backend -> credit_note_backend
    # contra_number -> credit_note_number
    # /contra -> /credit_note
    # contra_data -> credit_note_data
    # contra_vouchers -> credit_notes
    # contra -> credit_note (variables)
    # Contra -> Credit Note
    
    # Manual replacements to ensure correctness
    content = content.replace('contra_backend', 'credit_note_backend')
    content = content.replace('contra_number', 'credit_note_number')
    content = content.replace('contra_vouchers', 'credit_notes')
    content = content.replace('contra_data', 'credit_note_data')
    content = content.replace('contra_date', 'credit_note_date')
    content = content.replace('contraData', 'creditNoteData')
    
    # Capitalized / Title representations
    content = content.replace('Contra details', 'Credit Note details')
    content = content.replace('Contra Details', 'Credit Note Details')
    content = content.replace('Contra Vouchers', 'Credit Note Vouchers')
    content = content.replace('Contra \'', 'Credit Note \'')
    content = content.replace('Contra "', 'Credit Note "')
    content = content.replace('Contra', 'Credit Note')
    
    # API endpoints and smaller things
    content = content.replace('/api/contra/', '/api/credit_note/')
    content = content.replace('/api/db/contra', '/api/db/credit_notes')
    content = content.replace('href="/contra"', 'href="/credit_note"')
    
    # Python definitions and variables
    content = content.replace('def get_all_contra_data', 'def get_all_credit_note_data')
    content = content.replace('fetch_tally_contra', 'fetch_tally_credit_note')
    content = content.replace('sync_contra_to_zoho', 'sync_credit_note_to_zoho')
    content = content.replace('_fetch_day_contra', '_fetch_day_credit_note')
    
    # General words remaining:
    content = content.replace('contras =', 'credit_notes =')
    content = content.replace('contras)', 'credit_notes)')
    content = content.replace('contras.', 'credit_notes.')
    content = content.replace('contras[', 'credit_notes[')
    content = content.replace('for contra in', 'for credit_note in')
    content = content.replace('contra =', 'credit_note =')
    content = content.replace('allContras', 'allCreditNotes')
    content = content.replace('total-contras', 'total-credit-notes')
    content = content.replace('Total Contras', 'Total Credit Notes')
    content = content.replace('renderContras', 'renderCreditNotes')
    content = content.replace('contraModal', 'creditNoteModal')
    content = content.replace('contraDetails', 'creditNoteDetails')
    content = content.replace('contra:', 'credit_note:')
    content = content.replace('contra.html', 'credit_note.html')
    
    # Any other lower cases
    content = content.replace('contra', 'credit_note')
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

replace_in_file('credit_note/credit_note_backend.py')
replace_in_file('templates/credit_note.html')
print("Replacements done.")

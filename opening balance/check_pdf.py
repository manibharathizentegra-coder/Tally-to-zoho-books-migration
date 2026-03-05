import pdfplumber

with pdfplumber.open(r'c:\Users\Zen\Desktop\Software_With_Front_END\opening balance\new_TrialBal.pdf') as pdf:
    page = pdf.pages[0]
    words = page.extract_words(keep_blank_chars=True, x_tolerance=3, y_tolerance=3)
    for w in words[:40]:
        print(f"  x0={w['x0']:.1f}  x1={w['x1']:.1f}  top={w['top']:.1f}  text={w['text']!r}")

"""
Quick test: does edgartools return clean financial data for META?
Run on Railway: python3 test_edgar.py
"""
from edgar import Company, set_identity
set_identity("research@sbctracker.io")

company = Company("META")
print(f"Company: {company.name} (CIK: {company.cik})")

filing = company.get_filings(form="10-K").latest()
print(f"Latest: filed {filing.filing_date}, period {filing.period_of_report}")

xbrl = filing.xbrl()
print(f"xbrl type: {type(xbrl)}")
print(f"xbrl.statements type: {type(xbrl.statements)}")

# ── Latest filing ─────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("INCOME STATEMENT (latest)")
print("="*70)
try:
    inc = xbrl.statements.income_statement()
    df = inc.to_dataframe()
    print(f"Columns: {list(df.columns)}")
    if 'label' in df.columns and 'value' in df.columns:
        print(df[['label', 'value']].to_string())
    else:
        print(df.to_string())
except Exception as e:
    print(f"ERROR: {e}")
    import traceback; traceback.print_exc()

print("\n" + "="*70)
print("CASH FLOW STATEMENT (latest)")
print("="*70)
try:
    cf = xbrl.statements.cash_flow_statement()
    df = cf.to_dataframe()
    print(f"Columns: {list(df.columns)}")
    if 'label' in df.columns and 'value' in df.columns:
        print(df[['label', 'value']].to_string())
    else:
        print(df.to_string())
except Exception as e:
    print(f"ERROR: {e}")
    import traceback; traceback.print_exc()

print("\n" + "="*70)
print("BALANCE SHEET (latest)")
print("="*70)
try:
    bs = xbrl.statements.balance_sheet()
    df = bs.to_dataframe()
    print(f"Columns: {list(df.columns)}")
    if 'label' in df.columns and 'value' in df.columns:
        print(df[['label', 'value']].to_string())
    else:
        print(df.to_string())
except Exception as e:
    print(f"ERROR: {e}")
    import traceback; traceback.print_exc()

# ── FY2019 ────────────────────────────────────────────────────────────────────
print("\n" + "="*70)
print("FY2019 CASH FLOW (expecting SBC ~$4.8B)")
print("="*70)
try:
    for f in company.get_filings(form="10-K"):
        if '2019' in str(f.period_of_report):
            print(f"Found: filed {f.filing_date}, period {f.period_of_report}")
            xbrl2 = f.xbrl()
            cf2 = xbrl2.statements.cash_flow_statement()
            df2 = cf2.to_dataframe()
            if 'label' in df2.columns and 'value' in df2.columns:
                print(df2[['label', 'value']].to_string())
            else:
                print(df2.to_string())
            break
    else:
        print("No 2019 filing found")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback; traceback.print_exc()

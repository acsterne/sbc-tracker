"""
Quick test: does edgartools return clean financial data for META?
Run on Railway: python3 test_edgar.py
"""
from edgar import Company, set_identity
set_identity("research@sbctracker.io")

company = Company("META")
print(f"Company: {company.name} (CIK: {company.cik})")

filings = company.get_filings(form="10-K")
print(f"\n10-K filings found: {len(filings)}")

# Get the most recent 10-K
filing = filings[0]
print(f"\nLatest filing: {filing.filing_date} (period: {filing.period_of_report})")

tenk = filing.obj()
if not tenk:
    print("ERROR: filing.obj() returned None")
    exit(1)

print(f"\nHas financials: {hasattr(tenk, 'financials') and tenk.financials is not None}")

if tenk.financials:
    fin = tenk.financials

    print("\n" + "="*60)
    print("INCOME STATEMENT")
    print("="*60)
    try:
        inc = fin.income_statement
        if inc:
            df = inc.to_dataframe()
            print(df.to_string())
        else:
            print("income_statement is None")
    except Exception as e:
        print(f"ERROR: {e}")

    print("\n" + "="*60)
    print("CASH FLOW STATEMENT")
    print("="*60)
    try:
        cf = fin.cash_flow_statement
        if cf:
            df = cf.to_dataframe()
            print(df.to_string())
        else:
            print("cash_flow_statement is None")
    except Exception as e:
        print(f"ERROR: {e}")

    print("\n" + "="*60)
    print("BALANCE SHEET")
    print("="*60)
    try:
        bs = fin.balance_sheet
        if bs:
            df = bs.to_dataframe()
            print(df.to_string())
        else:
            print("balance_sheet is None")
    except Exception as e:
        print(f"ERROR: {e}")
else:
    print("No financials attribute on 10-K object")

# Also try getting an older filing (FY2019) to test historical
print("\n\n" + "="*60)
print("TESTING FY2019 FILING")
print("="*60)
for f in filings:
    if f.period_of_report and "2019" in str(f.period_of_report):
        print(f"Found: {f.filing_date} (period: {f.period_of_report})")
        tenk2 = f.obj()
        if tenk2 and tenk2.financials:
            cf = tenk2.financials.cash_flow_statement
            if cf:
                df = cf.to_dataframe()
                # Just print rows with "compensation" or "stock" in them
                for idx, row in df.iterrows():
                    label = str(idx).lower()
                    if "compens" in label or "stock" in label or "share" in label:
                        print(f"  {idx}: {row.to_dict()}")
        break

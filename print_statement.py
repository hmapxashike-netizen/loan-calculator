import json
from datetime import date
from statements import generate_customer_facing_statement

try:
    out, _ = generate_customer_facing_statement(1, as_of_date=date(2025, 1, 29))
    for row in out:
        print(f"{row['Due Date']} | {row['Narration']:<40} | D: {row['Debits']:<8} | C: {row['Credits']:<8} | B: {row['Balance']:<8}")
except Exception as e:
    print(f"Error: {e}")

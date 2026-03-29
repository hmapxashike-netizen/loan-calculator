import decimal

d1 = decimal.Decimal("3.5087719298")
f1 = float(d1)
print(f"Float representation: {f1}")

d2 = decimal.Decimal(str(f1))
q2 = d2.quantize(decimal.Decimal("0.01"), rounding=decimal.ROUND_HALF_UP)
print(f"Quantized 2dp: {q2}")
f2 = float(q2)
print(f"Formatted 2dp string: {f2:,.2f}")

print("Testing exact EOD truncation...")
from decimal import Decimal, ROUND_HALF_UP

amount = Decimal("3.5087719298")
# Inside accounting_service.py
line_amount = amount
line_amount = Decimal(str(line_amount)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
print(f"line_amount in service: {line_amount}")

# Inside accounting_dal.py (it's inserted into NUMERIC(28,10))
# Let's assume it inserts correctly.

# Inside app.py (retrieval)
debit = float(line_amount)
print(f"retrieved debit: {debit}")

def as_2dp(value):
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

print(f"App UI formatting: {as_2dp(debit)}")

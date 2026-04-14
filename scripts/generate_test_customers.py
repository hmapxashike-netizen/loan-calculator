import csv
import random

# Batch template columns match ui/customers.py (Batch Customer Capture).
ADDRESS_TYPES_INDIV = ("physical", "postal", "registered", "mailing")
ADDRESS_TYPES_CORP = ("registered", "physical", "postal", "head_office")
CITIES = ("Harare", "Bulawayo", "Mutare", "Gweru", "Masvingo", "Chitungwiza", "Kwekwe", "Marondera")


def generate_customers(num_clients: int, filename: str) -> None:
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "migration_ref",
                "customer_type",
                "full_name",
                "legal_name",
                "trading_name",
                "reg_number",
                "tin",
                "national_id",
                "employer_details",
                "phone1",
                "phone2",
                "email1",
                "email2",
                "sector_id",
                "sector_name",
                "subsector_id",
                "subsector_name",
                "address_type",
                "line1",
                "line2",
                "city",
                "region",
                "postal_code",
                "country",
            ]
        )

        for i in range(1, num_clients + 1):
            is_indiv = random.random() < 0.52

            if is_indiv:
                has_secondary_contact = random.random() < 0.35
                line2 = f"Flat {random.randint(1, 40)}" if random.random() < 0.25 else ""
                region = random.choice(("", "Mashonaland", "Matabeleland", "Manicaland", "Midlands"))
                postal = f"PO Box {random.randint(100, 9999)}" if random.random() < 0.2 else ""
                row = [
                    f"CUST-{i:04d}",
                    "individual",
                    f"Test Individual {i}",
                    "",
                    "",
                    "",
                    "",
                    f"{random.randint(10, 99)}-{random.randint(100000, 999999)}A{random.randint(10, 99)}",
                    f"Test Employer {i}" if random.random() < 0.9 else "",
                    f"+2637{random.randint(10000000, 99999999)}",
                    f"+2637{random.randint(10000000, 99999999)}" if has_secondary_contact else "",
                    f"indiv{i}@example.com",
                    f"indiv{i}.alt@example.com" if has_secondary_contact else "",
                    "",
                    "",
                    "",
                    "",
                    random.choice(ADDRESS_TYPES_INDIV),
                    f"{random.randint(1, 999)} Test Road",
                    line2,
                    random.choice(CITIES),
                    region,
                    postal,
                    "Zimbabwe",
                ]
            else:
                has_trading = random.random() < 0.85
                line2 = f"Building {random.choice('ABCDE')}" if random.random() < 0.3 else ""
                region = random.choice(("", "Harare Province", "Bulawayo Metro"))
                postal = "" if random.random() < 0.75 else f"{random.randint(1000, 99999)}"
                row = [
                    f"CUST-{i:04d}",
                    "corporate",
                    "",
                    f"Test Corporate {i} (Pvt) Ltd",
                    f"Test Corp {i}" if has_trading else "",
                    f"{random.randint(1000, 9999)}/{random.randint(2010, 2024)}",
                    f"200{random.randint(1000000, 9999999)}",
                    "",
                    "",
                    f"+2637{random.randint(10000000, 99999999)}",
                    f"+2637{random.randint(10000000, 99999999)}" if random.random() < 0.4 else "",
                    f"finance{i}@testcorp.co.zw",
                    f"accounts{i}@testcorp.co.zw" if random.random() < 0.25 else "",
                    "",
                    "",
                    "",
                    "",
                    random.choice(ADDRESS_TYPES_CORP),
                    f"Unit {random.randint(1, 99)} Industrial Park",
                    line2,
                    random.choice(CITIES),
                    region,
                    postal,
                    "Zimbabwe",
                ]
            writer.writerow(row)

    print(f"Successfully generated {num_clients} customers in {filename}")


if __name__ == "__main__":
    generate_customers(1000, "test_customers.csv")

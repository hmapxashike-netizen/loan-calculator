import csv
import random

def generate_customers(num_clients, filename):
    with open(filename, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "migration_ref", "customer_type", "full_name", "legal_name", "trading_name", "reg_number", "tin",
            "national_id", "employer_details", "phone1", "phone2", "email1", "email2",
            "sector_id", "sector_name", "subsector_id", "subsector_name",
            "address_type", "line1", "line2", "city", "region", "postal_code", "country"
        ])

        for i in range(1, num_clients + 1):
            is_indiv = random.choice([True, False])
            
            if is_indiv:
                row = [
                    f"CUST-{i:04d}",
                    "individual",
                    f"Test Individual {i}",
                    "", "", "", "",
                    f"{random.randint(10, 99)}-{random.randint(100000, 999999)}A{random.randint(10, 99)}",
                    f"Test Employer {i}",
                    f"+2637{random.randint(10000000, 99999999)}",
                    "",
                    f"indiv{i}@example.com",
                    "",
                    "", "", "", "",
                    "physical",
                    f"{random.randint(1, 999)} Test Road",
                    "",
                    random.choice(["Harare", "Bulawayo", "Mutare", "Gweru"]),
                    "", "", "Zimbabwe"
                ]
            else:
                row = [
                    f"CUST-{i:04d}",
                    "corporate",
                    "",
                    f"Test Corporate {i} (Pvt) Ltd",
                    f"Test Corp {i}",
                    f"{random.randint(1000, 9999)}/{random.randint(2010, 2024)}",
                    f"200{random.randint(1000000, 9999999)}",
                    "", "",
                    f"+2637{random.randint(10000000, 99999999)}",
                    "",
                    f"finance{i}@testcorp.co.zw",
                    "",
                    "", "", "", "",
                    "registered",
                    f"Unit {random.randint(1, 99)} Industrial Park",
                    "",
                    random.choice(["Harare", "Bulawayo", "Mutare", "Gweru"]),
                    "", "", "Zimbabwe"
                ]
            writer.writerow(row)

    print(f"Successfully generated {num_clients} customers in {filename}")

if __name__ == "__main__":
    generate_customers(100, "test_customers.csv")

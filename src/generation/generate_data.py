
from pathlib import Path
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import random

fake = Faker("en_US")
Faker.seed(42)
random.seed(42)

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "data" / "source"

CONFIG = {
    "retailmax": {
        "prefix": "RM",
        "customers": 5000,
        "products": 1000,
        "stores": 50,
        "sales": 50000,
        "inventory": 2000,
        "suppliers": 100,
    },
    "freshmart": {
        "prefix": "FM",
        "customers": 4000,
        "products": 800,
        "stores": 40,
        "sales": 40000,
        "inventory": 1600,
        "suppliers": 80,
    },
}

CATEGORIES = {
    "retailmax": [
        "Electronics", "Clothing", "Home",
        "Groceries", "Beauty", "Sports"
    ],
    "freshmart": [
        "Produce", "Dairy", "Bakery",
        "Meat", "Frozen", "Beverages",
        "Household", "Pantry"
    ],
}

PRODUCT_NAMES = {
    "Electronics": ["Wireless Headphones", "Bluetooth Speaker", "USB Charger"],
    "Clothing": ["Cotton T-Shirt", "Denim Jeans", "Winter Jacket"],
    "Home": ["Table Lamp", "Storage Basket", "Bed Sheet"],
    "Groceries": ["Rice Bag", "Pasta", "Cooking Oil"],
    "Beauty": ["Shampoo", "Face Wash", "Body Lotion"],
    "Sports": ["Yoga Mat", "Water Bottle", "Resistance Band"],
    "Produce": ["Apples", "Bananas", "Spinach"],
    "Dairy": ["Whole Milk", "Greek Yogurt", "Cheddar Cheese"],
    "Bakery": ["Sourdough Bread", "Bagels", "Muffins"],
    "Meat": ["Chicken Breast", "Ground Beef", "Turkey"],
    "Frozen": ["Frozen Pizza", "Mixed Vegetables", "Ice Cream"],
    "Beverages": ["Orange Juice", "Sparkling Water", "Coffee"],
    "Household": ["Paper Towels", "Dish Soap", "Laundry Detergent"],
    "Pantry": ["Canned Beans", "Oatmeal", "Peanut Butter"],
}

def random_date(start="2023-01-01", end="2026-09-30"):
    return fake.date_between(
        start_date=datetime.strptime(start, "%Y-%m-%d").date(),
        end_date=datetime.strptime(end, "%Y-%m-%d").date()
    )

def format_date(value, company):
    if company == "retailmax":
        return value.strftime("%Y-%m-%d")
    return value.strftime("%m/%d/%Y")

def generate_suppliers(company, cfg):
    rows = []
    for i in range(1, cfg["suppliers"] + 1):
        sid = f"{cfg['prefix']}-SUP{i:04d}"
        name = fake.company()
        email = fake.company_email()
        phone = fake.phone_number()
        city = fake.city()
        state = fake.state_abbr()

        if company == "retailmax":
            rows.append({
                "supplier_id": sid,
                "supplier_name": name,
                "contact_email": email,
                "phone": phone,
                "city": city,
                "state": state,
            })
        else:
            rows.append({
                "vendor_code": sid,
                "vendor_name": name,
                "email_address": email,
                "contact_number": phone,
                "city_name": city,
                "state_code": state,
            })
    return pd.DataFrame(rows)

def generate_customers(company, cfg):
    rows = []
    for i in range(1, cfg["customers"] + 1):
        cid = (
            f"RM-C{i:05d}" if company == "retailmax"
            else f"FM{10000+i}"
        )
        first = fake.first_name()
        last = fake.last_name()
        email = fake.email()
        phone = fake.phone_number()
        city = fake.city()
        state = fake.state_abbr()
        created = format_date(random_date(), company)

        if company == "retailmax":
            rows.append({
                "customer_id": cid,
                "first_name": first,
                "last_name": last,
                "email": email,
                "phone": phone,
                "city": city,
                "state": state,
                "created_at": created,
            })
        else:
            rows.append({
                "cust_no": cid,
                "fname": first,
                "lname": last,
                "email_address": email,
                "mobile_no": phone,
                "city_name": city,
                "state_code": state,
                "signup_date": created,
            })
    return pd.DataFrame(rows)

def generate_stores(company, cfg):
    rows = []
    for i in range(1, cfg["stores"] + 1):
        sid = f"{cfg['prefix']}-ST{i:04d}"
        city = fake.city()
        state = fake.state_abbr()
        zip_code = fake.postcode()
        opened = format_date(random_date("2015-01-01"), company)

        if company == "retailmax":
            rows.append({
                "store_id": sid,
                "store_name": f"RetailMax {city} {i}",
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "opened_date": opened,
            })
        else:
            rows.append({
                "location_code": sid,
                "location_name": f"FreshMart {city} {i}",
                "city_name": city,
                "state_code": state,
                "postal_code": zip_code,
                "launch_date": opened,
            })
    return pd.DataFrame(rows)

def generate_products(company, cfg, suppliers):
    rows = []
    supplier_key = (
        "supplier_id" if company == "retailmax"
        else "vendor_code"
    )
    supplier_ids = suppliers[supplier_key].tolist()

    for i in range(1, cfg["products"] + 1):
        pid = f"{cfg['prefix']}-P{i:05d}"
        category = random.choice(CATEGORIES[company])
        name = random.choice(PRODUCT_NAMES[category])
        brand = fake.company().split()[0]
        price = round(random.uniform(2, 300), 2)
        supplier = random.choice(supplier_ids)
        created = format_date(random_date(), company)

        if company == "retailmax":
            rows.append({
                "product_id": pid,
                "product_name": name,
                "category": category,
                "brand": brand,
                "unit_price": price,
                "supplier_id": supplier,
                "created_at": created,
            })
        else:
            rows.append({
                "sku_code": pid,
                "item_description": name,
                "department": category,
                "brand_name": brand,
                "selling_price": price,
                "vendor_code": supplier,
                "listed_date": created,
            })
    return pd.DataFrame(rows)

def generate_sales(company, cfg, customers, products, stores):
    rows = []
    if company == "retailmax":
        customer_ids = customers["customer_id"].tolist()
        product_ids = products["product_id"].tolist()
        store_ids = stores["store_id"].tolist()
        price_map = dict(zip(products["product_id"], products["unit_price"]))
    else:
        customer_ids = customers["cust_no"].tolist()
        product_ids = products["sku_code"].tolist()
        store_ids = stores["location_code"].tolist()
        price_map = dict(zip(products["sku_code"], products["selling_price"]))

    for i in range(1, cfg["sales"] + 1):
        cid = random.choice(customer_ids)
        pid = random.choice(product_ids)
        sid = random.choice(store_ids)
        qty = random.randint(1, 5)
        price = price_map[pid]
        discount = round(qty * price * random.choice([0, 0, 0.05, 0.10]), 2)
        total = round(qty * price - discount, 2)
        date = format_date(random_date(), company)

        if company == "retailmax":
            rows.append({
                "transaction_id": f"RM-T{i:07d}",
                "customer_id": cid,
                "product_id": pid,
                "store_id": sid,
                "transaction_date": date,
                "quantity": qty,
                "unit_price": price,
                "discount_amount": discount,
                "total_amount": total,
            })
        else:
            rows.append({
                "receipt_no": f"FM-R{i:07d}",
                "cust_no": cid,
                "sku_code": pid,
                "location_code": sid,
                "sale_date": date,
                "units_sold": qty,
                "price_per_unit": price,
                "discount_value": discount,
                "net_sales": total,
            })
    return pd.DataFrame(rows)

def generate_inventory(company, cfg, products, stores):
    rows = []
    product_key = "product_id" if company == "retailmax" else "sku_code"
    store_key = "store_id" if company == "retailmax" else "location_code"

    product_ids = products[product_key].tolist()
    store_ids = stores[store_key].tolist()

    # Each inventory row represents one product at one store.
    combinations = random.sample(
        [(p, s) for p in product_ids for s in store_ids],
        cfg["inventory"]
    )

    for i, (pid, sid) in enumerate(combinations, start=1):
        qty = random.randint(0, 500)
        reorder = random.randint(20, 100)
        updated = format_date(random_date(), company)

        if company == "retailmax":
            rows.append({
                "inventory_id": f"RM-I{i:06d}",
                "product_id": pid,
                "store_id": sid,
                "stock_quantity": qty,
                "reorder_level": reorder,
                "last_updated": updated,
            })
        else:
            rows.append({
                "stock_record_id": f"FM-I{i:06d}",
                "sku_code": pid,
                "location_code": sid,
                "available_units": qty,
                "minimum_stock": reorder,
                "stock_update_date": updated,
            })
    return pd.DataFrame(rows)

def save_dataset(company, name, df):
    folder = OUTPUT / company
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"Created {path.relative_to(ROOT)}: {len(df):,} rows")

def main():
    for company, cfg in CONFIG.items():
        print(f"\nGenerating data for {company.upper()}")

        suppliers = generate_suppliers(company, cfg)
        customers = generate_customers(company, cfg)
        stores = generate_stores(company, cfg)
        products = generate_products(company, cfg, suppliers)
        sales = generate_sales(company, cfg, customers, products, stores)
        inventory = generate_inventory(company, cfg, products, stores)

        datasets = {
            "customers": customers,
            "products": products,
            "stores": stores,
            "sales": sales,
            "inventory": inventory,
            "suppliers": suppliers,
        }

        for name, df in datasets.items():
            save_dataset(company, name, df)

    print("\nSUCCESS: All 12 source datasets generated!")

if __name__ == "__main__":
    main()

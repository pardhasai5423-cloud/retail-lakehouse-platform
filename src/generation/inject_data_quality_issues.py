
from pathlib import Path
import json
import random
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "data" / "source"
OUTPUT = ROOT / "data" / "source_dirty"
REPORT = ROOT / "data" / "quality_injection_report.json"

random.seed(42)

COMPANIES = ["retailmax", "freshmart"]
DATASETS = [
    "customers", "products", "stores",
    "sales", "inventory", "suppliers"
]

CUSTOMER_COLS = {
    "retailmax": {
        "id": "customer_id",
        "email": "email",
        "first": "first_name",
        "last": "last_name",
        "phone": "phone",
        "city": "city",
        "state": "state",
    },
    "freshmart": {
        "id": "cust_no",
        "email": "email_address",
        "first": "fname",
        "last": "lname",
        "phone": "mobile_no",
        "city": "city_name",
        "state": "state_code",
    },
}

SALES_COLS = {
    "retailmax": {
        "product": "product_id",
        "quantity": "quantity",
        "date": "transaction_date",
    },
    "freshmart": {
        "product": "sku_code",
        "quantity": "units_sold",
        "date": "sale_date",
    },
}

def choose_rows(df, count, exclude=None):
    """Select unique row positions, optionally excluding some."""
    excluded = set(exclude or [])
    candidates = [i for i in range(len(df)) if i not in excluded]
    return random.sample(candidates, min(count, len(candidates)))

def inject_customer_issues(company, df, report):
    cols = CUSTOMER_COLS[company]
    df = df.copy()

    # 2% missing customer emails
    missing_rows = choose_rows(df, round(len(df) * 0.02))
    df.loc[missing_rows, cols["email"]] = ""
    report["missing_customer_emails"] = len(missing_rows)

    # 1% duplicated customer records, preserving source IDs
    duplicate_rows = choose_rows(df, round(len(df) * 0.01))
    duplicates = df.iloc[duplicate_rows].copy()
    df = pd.concat([df, duplicates], ignore_index=True)
    report["duplicate_customer_rows_added"] = len(duplicates)

    return df

def inject_product_issues(company, df, report):
    df = df.copy()
    price_col = (
        "unit_price" if company == "retailmax"
        else "selling_price"
    )

    missing_rows = choose_rows(df, round(len(df) * 0.01))
    df.loc[missing_rows, price_col] = None
    report["missing_product_prices"] = len(missing_rows)

    return df

def inject_sales_issues(company, df, report):
    df = df.copy()
    cols = SALES_COLS[company]

    # 0.5% invalid product references
    bad_product_rows = choose_rows(df, round(len(df) * 0.005))
    for i in bad_product_rows:
        df.at[i, cols["product"]] = f"INVALID-PRODUCT-{i}"
    report["invalid_product_references"] = len(bad_product_rows)

    # 0.5% negative quantities, distinct from invalid references
    negative_rows = choose_rows(
        df, round(len(df) * 0.005),
        exclude=bad_product_rows
    )
    df.loc[negative_rows, cols["quantity"]] = (
        -pd.to_numeric(
            df.loc[negative_rows, cols["quantity"]],
            errors="raise"
        )
    ).astype(int).astype(str)
    report["negative_sales_quantities"] = len(negative_rows)

    # 1% inconsistent date formats, separate from previous issues
    date_rows = choose_rows(
        df, round(len(df) * 0.01),
        exclude=bad_product_rows + negative_rows
    )

    for i in date_rows:
        value = df.at[i, cols["date"]]
        try:
            if company == "retailmax":
                parsed = pd.to_datetime(value, format="%Y-%m-%d")
                df.at[i, cols["date"]] = parsed.strftime("%d-%b-%Y")
            else:
                parsed = pd.to_datetime(value, format="%m/%d/%Y")
                df.at[i, cols["date"]] = parsed.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

    report["inconsistent_sales_dates"] = len(date_rows)

    return df

def inject_cross_company_customers(data, report):
    """
    Make 100 FreshMart customers represent the same people
    as RetailMax customers, while keeping their own source IDs.
    """
    rm = data["retailmax"]["customers"]
    fm = data["freshmart"]["customers"]

    rm_cols = CUSTOMER_COLS["retailmax"]
    fm_cols = CUSTOMER_COLS["freshmart"]

    # Exclude duplicate customer IDs to select stable source records.
    rm_unique = rm.drop_duplicates(subset=[rm_cols["id"]])
    fm_unique = fm.drop_duplicates(subset=[fm_cols["id"]])

    count = min(100, len(rm_unique), len(fm_unique))

    rm_sample = rm_unique.sample(n=count, random_state=42)
    fm_sample = fm_unique.sample(n=count, random_state=42)

    for (_, rm_row), (fm_idx, _) in zip(
        rm_sample.iterrows(), fm_sample.iterrows()
    ):
        for field in ["first", "last", "email", "phone", "city", "state"]:
            fm.at[fm_idx, fm_cols[field]] = rm_row[rm_cols[field]]

    data["freshmart"]["customers"] = fm
    report["cross_company_customer_matches_created"] = count

def main():
    data = {}
    report = {}

    for company in COMPANIES:
        data[company] = {}
        report[company] = {}

        for name in DATASETS:
            path = SOURCE / company / f"{name}.csv"
            if not path.exists():
                raise FileNotFoundError(
                    f"Missing {path}. Run generate_data.py first."
                )

            data[company][name] = pd.read_csv(
                path, dtype=str, keep_default_na=False
            )

        data[company]["customers"] = inject_customer_issues(
            company,
            data[company]["customers"],
            report[company]
        )

        data[company]["products"] = inject_product_issues(
            company,
            data[company]["products"],
            report[company]
        )

        data[company]["sales"] = inject_sales_issues(
            company,
            data[company]["sales"],
            report[company]
        )

    # Overlap identities between the two independent source systems.
    report["cross_company"] = {}
    inject_cross_company_customers(data, report["cross_company"])

    # Write all 12 datasets, including unchanged store/inventory/supplier files.
    for company in COMPANIES:
        folder = OUTPUT / company
        folder.mkdir(parents=True, exist_ok=True)

        for name in DATASETS:
            path = folder / f"{name}.csv"
            data[company][name].to_csv(path, index=False)
            print(f"Created {path.relative_to(ROOT)}")

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2))
    print(f"\nQuality report: {REPORT.relative_to(ROOT)}")
    print(json.dumps(report, indent=2))
    print("\nSUCCESS: Dirty source datasets generated!")

if __name__ == "__main__":
    main()

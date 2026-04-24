import pandas as pd
import re
from datetime import datetime

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.

WORD_TO_NUMBER = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10
}


def _parse_price(raw_price):
    if raw_price is None:
        return None

    price_text = str(raw_price).strip()
    if not price_text or price_text.lower() in {"n/a", "null", "liên hệ", "nan", "none"}:
        return None

    # Clean literal USD amounts and common text forms
    if price_text.startswith("$"):
        price_text = price_text[1:]

    match = re.match(r"^(?P<words>[a-zA-Z]+)\s*dollars?$", price_text, flags=re.IGNORECASE)
    if match:
        word = match.group("words").lower()
        return float(WORD_TO_NUMBER.get(word, 0)) if word in WORD_TO_NUMBER else None

    numeric = re.sub(r"[^0-9.-]", "", price_text)
    if not numeric or numeric == "-":
        return None

    try:
        return float(numeric)
    except ValueError:
        return None


def _normalize_date(raw_date):
    if raw_date is None:
        return None

    date_text = str(raw_date).strip()
    if not date_text or date_text.lower() in {"nan", "none"}:
        return None

    if re.match(r"^\d{4}[-/]\d{2}[-/]\d{2}$", date_text):
        parsed = pd.to_datetime(date_text, errors='coerce', dayfirst=False)
    else:
        parsed = pd.to_datetime(date_text, errors='coerce', dayfirst=True)

    if pd.isna(parsed):
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y/%m/%d",
            "%B %dth %Y",
            "%d %b %Y",
            "%d %B %Y"
        ]
        for fmt in formats:
            try:
                parsed = datetime.strptime(date_text, fmt)
                break
            except ValueError:
                continue

    if parsed is None or pd.isna(parsed):
        return None

    return parsed.strftime("%Y-%m-%d")


def process_sales_csv(file_path):
    # --- FILE READING (Handled for students) ---
    df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
    # ------------------------------------------

    df = df.drop_duplicates(subset=['id'], keep='first')
    documents = []

    for _, row in df.iterrows():
        row_id = str(row.get('id', '')).strip()
        if not row_id:
            continue

        price = _parse_price(row.get('price'))
        normalized_date = _normalize_date(row.get('date_of_sale'))

        content = (
            f"Sale record {row_id}: {row.get('product_name', '').strip()} sold on {normalized_date or 'unknown date'} "
            f"for {price if price is not None else row.get('price')} {row.get('currency', '').strip()}."
        )

        documents.append({
            "document_id": f"csv-{row_id}",
            "content": content,
            "source_type": "CSV",
            "author": row.get('seller_id', 'Unknown'),
            "timestamp": None,
            "source_metadata": {
                "original_file": file_path,
                "product_name": row.get('product_name'),
                "category": row.get('category'),
                "price": price,
                "currency": row.get('currency'),
                "date_of_sale": normalized_date,
                "seller_id": row.get('seller_id'),
                "stock_quantity": row.get('stock_quantity')
            }
        })

    return documents


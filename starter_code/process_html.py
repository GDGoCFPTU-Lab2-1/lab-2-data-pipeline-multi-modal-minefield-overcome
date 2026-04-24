import re
from bs4 import BeautifulSoup

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract product data from the HTML table, ignoring boilerplate.

def parse_html_catalog(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
    # ------------------------------------------

    table = soup.find('table', id='main-catalog')
    if table is None:
        return []

    tbody = table.find('tbody')
    if tbody is None:
        return []

    docs = []
    rows = tbody.find_all('tr')
    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all('td')]
        if len(cells) != 6:
            continue

        product_id, name, category, price_text, stock_text, review_text = cells
        price = None
        if price_text.lower() not in {'n/a', 'liên hệ', 'null', ''}:
            numeric_part = re.sub(r"[^0-9.-]", "", price_text)
            try:
                price = float(numeric_part) if numeric_part else None
            except ValueError:
                price = None

        stock = None
        try:
            stock = int(stock_text) if stock_text and stock_text != '-' else None
        except ValueError:
            stock = None

        content = (
            f"Product {product_id}: {name} in category {category}. "
            f"Listed price: {price_text}. Stock: {stock_text}. Rating: {review_text}."
        )

        docs.append({
            "document_id": f"html-{product_id}",
            "content": content,
            "source_type": "HTML",
            "author": "Unknown",
            "timestamp": None,
            "source_metadata": {
                "original_file": file_path,
                "product_id": product_id,
                "name": name,
                "category": category,
                "price_vnd": price,
                "stock": stock,
                "rating": review_text
            }
        })

    return docs


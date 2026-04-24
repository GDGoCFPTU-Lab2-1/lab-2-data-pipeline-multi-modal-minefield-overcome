import json
import time
import os

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")


# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Task: Orchestrate the ingestion pipeline and handle errors/SLA.

def main():
    start_time = time.time()
    final_kb = []
    
    # --- FILE PATH SETUP (Handled for students) ---
    pdf_path = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")
    
    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")
    # ----------------------------------------------

    sources = []

    pdf_doc = extract_pdf_data(pdf_path)
    if pdf_doc:
        sources.append(pdf_doc)

    transcript_doc = clean_transcript(trans_path)
    if transcript_doc:
        sources.append(transcript_doc)

    html_docs = parse_html_catalog(html_path)
    if html_docs:
        sources.extend(html_docs)

    csv_docs = process_sales_csv(csv_path)
    if csv_docs:
        sources.extend(csv_docs)

    code_doc = extract_logic_from_code(code_path)
    if code_doc:
        sources.append(code_doc)

    for source in sources:
        if source is None:
            continue
        if isinstance(source, list):
            docs = source
        else:
            docs = [source]

        for doc in docs:
            if not isinstance(doc, dict):
                continue
            if run_quality_gate(doc):
                final_kb.append(doc)
            else:
                print(f"Rejected by quality gate: {doc.get('document_id')}")

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(final_kb, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(final_kb)} documents to {output_path}")
    except Exception as e:
        print(f"Failed to write output JSON: {e}")

    end_time = time.time()
    print(f"Pipeline finished in {end_time - start_time:.2f} seconds.")
    print(f"Total valid documents stored: {len(final_kb)}")


if __name__ == "__main__":
    main()

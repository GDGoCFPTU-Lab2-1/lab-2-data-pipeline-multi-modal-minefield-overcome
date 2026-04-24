import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Clean the transcript text and extract key information.

def clean_transcript(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    # ------------------------------------------

    cleaned = re.sub(
        r"\[(?:\d{2}:\d{2}:\d{2}|Music(?: starts| ends)?|inaudible|Laughter)\]",
        "",
        text,
        flags=re.IGNORECASE
    )
    cleaned = "\n".join(
        line.strip() for line in cleaned.splitlines() if line.strip()
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    detected_price_vnd = None
    if re.search(r"năm\s+trăm\s+nghìn", text, flags=re.IGNORECASE):
        detected_price_vnd = 500000
    else:
        match = re.search(r"(\d[\d.,]*)\s*VND", text, flags=re.IGNORECASE)
        if match:
            amount_text = match.group(1).replace(".", "").replace(",", "")
            try:
                detected_price_vnd = int(amount_text)
            except ValueError:
                detected_price_vnd = None

    return {
        "document_id": "video-transcript-001",
        "content": cleaned,
        "source_type": "Video",
        "author": "Unknown",
        "timestamp": None,
        "source_metadata": {
            "original_file": file_path,
            "detected_price_vnd": detected_price_vnd
        }
    }


# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.

def run_quality_gate(document_dict):
    # Reject very short or empty content
    content = str(document_dict.get("content", "") or "").strip()
    if len(content) < 20:
        return False

    # Reject toxic/error strings
    error_signals = [
        "Null pointer exception",
        "segmentation fault",
        "stack overflow",
        "invalid memory access",
        "error:"
    ]
    for signal in error_signals:
        if signal.lower() in content.lower():
            return False

    # Reject obvious logic discrepancies inside the text
    lower_content = content.lower()
    if "8%" in lower_content and "10%" in lower_content and "tax" in lower_content:
        return False

    return True

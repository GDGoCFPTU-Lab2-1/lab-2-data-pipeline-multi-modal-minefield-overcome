import ast
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code.

def extract_logic_from_code(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()
    # ------------------------------------------

    tree = ast.parse(source_code)
    function_entries = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef):
            function_entries.append({
                "name": node.name,
                "docstring": ast.get_docstring(node) or "No docstring provided."
            })

    business_rules = [
        match.group(0).strip()
        for match in re.finditer(r"#\s*Business Logic Rule\s*\d{3}:?.*", source_code)
    ]

    content_lines = []
    if function_entries:
        for entry in function_entries:
            content_lines.append(
                f"Function {entry['name']}: {entry['docstring']}"
            )
    if business_rules:
        content_lines.append("Extracted business rules:")
        content_lines.extend(business_rules)
    if not content_lines:
        content_lines.append("No legacy code documentation or business rules could be extracted.")

    return {
        "document_id": "code-legacy-001",
        "content": "\n".join(content_lines),
        "source_type": "Code",
        "author": "Legacy System",
        "timestamp": None,
        "source_metadata": {
            "original_file": file_path,
            "function_count": len(function_entries),
            "business_rules_found": business_rules,
        }
    }


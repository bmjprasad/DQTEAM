from __future__ import annotations

from typing import Optional, Tuple


def parse_table_identifier(full_name: str) -> Tuple[Optional[str], str, str]:
    parts = full_name.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    raise ValueError(f"Expect 2 or 3 part name, got: {full_name}")
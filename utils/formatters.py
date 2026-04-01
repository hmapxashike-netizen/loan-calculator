def parse_display_substrings_csv(text: str) -> list[str]:
    return [p.strip().lower() for p in (text or "").split(",") if p.strip()]

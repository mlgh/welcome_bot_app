import html
import re
from typing import Mapping


class safe_html_str(str):
    """A string that's safe to render in html."""

    pass


def escape_html(s: str) -> safe_html_str:
    return safe_html_str(html.escape(s))


def safe_html_format(
    s: safe_html_str, dct: Mapping[str, safe_html_str]
) -> safe_html_str:
    if not isinstance(s, safe_html_str):
        raise ValueError(f"Value {s} is not safe.")
    for k, v in dct.items():
        if not isinstance(v, safe_html_str):
            raise ValueError(f"Value {v} for key {k} is not safe.")
    return safe_html_str(s.format(**dct))


def substitute_html(
    text: safe_html_str, substitutions: Mapping[str, safe_html_str]
) -> safe_html_str:
    parts = re.split(r"(\$[A-Z_]+)", text)
    body: list[safe_html_str] = []
    for part in parts:
        if part.startswith("$") and part[1:] in substitutions:
            body.append(substitutions[part[1:]])
        else:
            body.append(safe_html_str(part))
    return safe_html_str("".join(body))

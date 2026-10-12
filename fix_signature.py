"""Add `parameter_names: None,` to Signature struct literals (DF55 field)."""
import re
from pathlib import Path

ROOT = Path("/root/embucket/crates/functions/src")

# Match:  signature: Signature { ... volatility: Volatility::Xxx,? \n<indent>}
# Signature literals here end right before a line that closes with `},` at the
# same indent as the `signature: Signature {` line.
PATTERN = re.compile(
    r"(?P<open>[ \t]*signature: Signature \{\n"
    r"(?:[^\n]*\n)*?"
    r"(?P<indent>[ \t]*)(volatility: Volatility::[A-Za-z]+,?)\n"
    r"(?P<close>[ \t]*\},)\n)",
    re.M,
)


def add_field(m: "re.Match[str]") -> str:
    body_indent = m.group("indent")
    body = m.group(0)
    # insert parameter_names after volatility line
    vol_line = f"{body_indent}{m.group(3)}\n"
    insert = f"{body_indent}parameter_names: None,\n"
    return body.replace(vol_line, vol_line + insert, 1)


total = 0
for path in ROOT.rglob("*.rs"):
    text = path.read_text()
    new_text, n = PATTERN.subn(add_field, text)
    if n:
        path.write_text(new_text)
        total += n

print(f"patched initializers: {total}")

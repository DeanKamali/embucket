"""Remove now-unneeded `fn as_any` trait-method overrides for DataFusion 55.

DF 55 removed `as_any` from ScalarUDFImpl/AggregateUDFImpl/WindowUDFImpl/
TableProvider/ExecutionPlan (provided by blanket impls now). The overrides are
uniform: `fn as_any(&self) -> &dyn Any { self }` possibly with doc comments and
a trailing blank line.
"""
import re
import sys
from pathlib import Path

ROOT = Path("/root/embucket/crates/functions/src")

# fn as_any(&self) -> &dyn Any { self }   (allow fully-qualified Any too)
PATTERN = re.compile(
    r"\n[ \t]*(?:/// [^\n]*\n[ \t]*)*"
    r"fn as_any\(&self\) -> &dyn (?:std::any::)?Any \{\n"
    r"[ \t]*self\n"
    r"[ \t]*\}\n?",
    re.M,
)

removed = 0
for path in ROOT.rglob("*.rs"):
    text = path.read_text()
    new_text, n = PATTERN.subn("\n", text)
    if n:
        path.write_text(new_text)
        removed += n
        print(f"{path.name}: -{n}")

print(f"total removed: {removed}")

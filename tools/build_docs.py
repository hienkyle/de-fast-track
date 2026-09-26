"""Build the MkDocs source folder (docs/) from the study guides in the repo root.

The guides stay where they are; this script copies them into docs/ with
URL-friendly names, a clean sidebar title, and generates the home page.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"


def slug(text: str) -> str:
    text = text.lower().replace("&", "and")
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def main() -> None:
    DOCS.mkdir(exist_ok=True)
    for old in DOCS.glob("*.md"):
        old.unlink()

    rows = []
    for src in sorted(ROOT.glob("Unit *.md")):
        body = src.read_text(encoding="utf-8")
        # "Unit 03 - Relational DB & Advanced SQL.md" -> number + name
        m = re.match(r"Unit (\d+) - (.+)\.md$", src.name)
        num, name = m.group(1), m.group(2)
        title = f"Unit {num} — {name}"
        dest = DOCS / f"{slug(src.stem)}.md"
        dest.write_text(f"---\ntitle: \"{title}\"\n---\n\n{body}", encoding="utf-8")
        scope = re.search(r"^\*\*Scope:\*\*\s*(.+)$", body, re.M)
        rows.append((num, name, dest.stem, scope.group(1) if scope else ""))

    lines = [
        "---", "title: Home", "hide:", "  - navigation", "  - toc", "---", "",
        "# DE Fast Track — Study Guides", "",
        "Theory-focused study guides for the data engineering class. "
        "Use the **search box** (or press `/`) to search across every unit.", "",
        "| Unit | Topic | Scope |", "|---|---|---|",
    ]
    for num, name, stem, scope in rows:
        lines.append(f"| {num} | [{name}]({stem}.md) | {scope} |")
    (DOCS / "index.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Built {len(rows)} guides into {DOCS}")


if __name__ == "__main__":
    main()

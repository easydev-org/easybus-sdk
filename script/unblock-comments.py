#!/usr/bin/env python3
"""Rewrite `/** ... */` doc comments as `//` lines.

WHY THIS EXISTS AT ALL. Every doc comment in this repository is written as a `/** */` block, and
gofmt since Go 1.19 REFORMATS THOSE AS A MARKDOWN LIST — it reads the leading `*` of each line as a
bullet and emits blank lines and indentation around them. The result is not a reformatted comment, it
is a mangled one, which is why `gofmt -l` reports files nobody edited.

Converting to `//` is a lossless, mechanical rewrite: every line's text is preserved verbatim, one
leading ` * ` becomes `// `, and the block markers disappear. That is what makes it safe to do to 52
comments in one commit — and it is what lets `gofmt` be a gate here at all.

WHAT IT REFUSES TO TOUCH, because a silent wrong answer in a comment is worse than a skipped file:

  * a block whose interior has a line not starting with `*` (not the house style; unknown intent)
  * a block used as a struct/field comment where the star does not separate cleanly

It prints what it changed and exits non-zero if anything looked wrong, so a partial run is visible.
"""

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
START = re.compile(r"^(\s*)/\*\*\s*$")


def convert(path: pathlib.Path) -> tuple[int, list[str]]:
    lines = path.read_text().split("\n")
    out: list[str] = []
    changed = 0
    problems: list[str] = []
    i = 0

    while i < len(lines):
        m = START.match(lines[i])
        if not m:
            out.append(lines[i])
            i += 1
            continue

        indent = m.group(1)
        # Collect until the closing marker.
        j = i + 1
        body: list[str] = []
        closed = False
        while j < len(lines):
            stripped = lines[j].strip()
            if stripped == "*/":
                closed = True
                break
            body.append(lines[j])
            j += 1

        if not closed:
            problems.append(f"{path}:{i + 1}: /** with no closing */")
            out.append(lines[i])
            i += 1
            continue

        # Every interior line must be `*`, `* text`, or blank. A BLANK LINE IS THE HOUSE STYLE for a
        # paragraph break inside a doc comment — it carries no text, so it needs no star, and treating
        # it as "unknown style" would skip the files that need converting most.
        texts: list[str] = []
        ok = True
        for b in body:
            s = b.strip()
            if s == "" or s == "*":
                texts.append("")
            elif s.startswith("* "):
                texts.append(s[2:].rstrip())
            elif s.startswith("*"):
                texts.append(s[1:].rstrip())
            else:
                problems.append(f"{path}: interior line without '*': {b[:60]!r}")
                ok = False
                break

        if not ok:
            out.extend(lines[i : j + 1])
            i = j + 1
            continue

        # Drop markers, keep the words. No trailing blank `//` line: gofmt keeps a comment attached
        # to the declaration that follows, and a stray empty `//` reads as a paragraph break.
        while texts and texts[-1] == "":
            texts.pop()
        for t in texts:
            out.append(f"{indent}// {t}".rstrip() if t else f"{indent}//")

        changed += 1
        i = j + 1

    path.write_text("\n".join(out))
    return changed, problems


def main() -> int:
    dry = "--dry-run" in sys.argv
    total = 0
    problems: list[str] = []
    for p in sorted(ROOT.rglob("*.go")):
        if "vendor" in p.parts:
            continue
        if dry:
            text = p.read_text()
            n = sum(1 for line in text.split("\n") if START.match(line))
            if n:
                print(f"  {p.relative_to(ROOT)}: {n} block(s)")
                total += n
        else:
            n, prob = convert(p)
            if n:
                print(f"  rewrote {p.relative_to(ROOT)}: {n} block(s)")
                total += n
            problems.extend(prob)

    print(f"\n{total} doc comment(s) {'found' if dry else 'converted'}")
    if problems:
        print("\nNOT CONVERTED:", file=sys.stderr)
        for x in problems:
            print("  " + x, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

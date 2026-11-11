"""Convert monitoring/RUNBOOK.md to Confluence wiki markup (Insert, Markup, Confluence wiki):

    python3 monitoring/md2confluence.py monitoring/RUNBOOK.md monitoring/RUNBOOK.confluence.txt

Covers the subset RUNBOOK.md uses: headings, paragraphs, tables, code blocks,
inline code, bold, in-page links. Each section gets an {anchor} with its name.
"""
import re
import sys

src = open(sys.argv[1]).read().splitlines()
out = []
in_code = False


def inline(t):
    t = re.sub(r"\[([^\]]+)\]\(#([a-z-]+)\)", r"[\1|#\2]", t)      # in-page links
    t = re.sub(r"`([^`]+)`", r"{{\1}}", t)                         # inline code
    t = re.sub(r"\*\*([^*]+)\*\*", r"*\1*", t)                     # bold
    return t


for line in src:
    if line.startswith("```"):
        if not in_code:
            lang = line[3:].strip() or "none"
            out.append("{code:language=%s}" % lang)
        else:
            out.append("{code}")
        in_code = not in_code
        continue
    if in_code:
        out.append(line)
        continue
    if line.startswith("# "):
        out.append("h1. " + line[2:])
    elif line.startswith("## "):
        name = line[3:].strip()
        out.append("{anchor:%s}" % name)          # stable anchor: #<name>
        out.append("h2. " + name)
    elif line.startswith("|---"):
        continue
    elif line.startswith("|"):
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        prev = out[-1] if out else ""
        sep = "||" if not prev.startswith("|") else "|"
        out.append(sep + sep.join(inline(c) for c in cells) + sep)
    elif line.strip() == "":
        out.append("")
    else:
        text = inline(line)
        # A new paragraph line starting with *Check:* / *Act:* stays on its own
        # line; any other wrapped line joins the paragraph (wiki markup turns a
        # newline into a visible line break)
        prev = out[-1] if out else ""
        if prev and not prev.startswith(("h1.", "h2.", "{", "|")) and not text.startswith("*"):
            out[-1] = prev + " " + text
        else:
            out.append(text)
open(sys.argv[2], "w").write("\n".join(out) + "\n")

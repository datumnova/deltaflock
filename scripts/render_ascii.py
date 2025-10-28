#!/usr/bin/env python3
"""Render an HTML file with colored spans to ANSI colored output in terminal.

Usage:
  python3 scripts/render_ascii.py [path/to/file.html]

Defaults to: src/app/ascii_art_1761640468512.html

This tries to parse <span style="color: #RRGGBB"> or rgb(...) and emits \x1b[38;2;R;G;Bm sequences.
"""

import re
import sys
import html
from pathlib import Path

DEFAULT = (
    Path(__file__).resolve().parents[1] / "src" / "app" / "ascii_art_1761640468512.html"
)

HEX_RE = re.compile(r"#([0-9a-fA-F]{6}|[0-9a-fA-F]{3})")
RGB_RE = re.compile(r"rgb\s*\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*\)")


def hex_to_rgb(h: str):
    if len(h) == 3:
        return tuple(int(c * 2, 16) for c in h)
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))


def style_color_to_rgb(style: str):
    # look for color: #xxxxxx or rgb(...) inside style
    m = HEX_RE.search(style)
    if m:
        return hex_to_rgb(m.group(1))
    m2 = RGB_RE.search(style)
    if m2:
        return (int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
    return None


def color_to_ansi_fg(rgb_tuple):
    r, g, b = rgb_tuple
    return f"\x1b[38;2;{r};{g};{b}m"


def replace_spans(s: str):
    # handle <span ...style="...">inner</span>
    span_re = re.compile(r"<span([^>]*)>(.*?)</span>", re.DOTALL | re.IGNORECASE)

    def repl(m):
        attrs = m.group(1)
        inner = m.group(2)
        # try to extract style attr
        style_m = re.search(r'style\s*=\s*"([^"]*)"', attrs, re.IGNORECASE)
        color = None
        if style_m:
            color = style_color_to_rgb(style_m.group(1))
        # also try color attr like <span color="#..."> or <font color="...">
        if color is None:
            color_attr_m = re.search(r'color\s*=\s*"([^"]*)"', attrs, re.IGNORECASE)
            if color_attr_m:
                coltxt = color_attr_m.group(1)
                hexm = HEX_RE.search(coltxt)
                if hexm:
                    color = hex_to_rgb(hexm.group(1))
                else:
                    rgbm = RGB_RE.search(coltxt)
                    if rgbm:
                        color = (
                            int(rgbm.group(1)),
                            int(rgbm.group(2)),
                            int(rgbm.group(3)),
                        )
        # If we found a color, wrap inner in ANSI; else return inner unmodified (but keep inner spans processed)
        processed_inner = replace_spans(inner)  # recursive to handle nested spans
        if color:
            return f"{color_to_ansi_fg(color)}{processed_inner}\x1b[0m"
        return processed_inner

    # apply until no spans remain or a safety limit
    prev = None
    cur = s
    limit = 10
    i = 0
    while cur != prev and i < limit:
        prev = cur
        cur = span_re.sub(repl, cur)
        i += 1
    return cur


def html_to_ansi(path: Path):
    raw = path.read_text(encoding="utf-8", errors="ignore")
    # Replace <br> with newline
    raw = re.sub(r"<br\s*/?>", "\n", raw, flags=re.IGNORECASE)
    # Some HTML may use <div> as line wrapper -> replace with newline
    raw = re.sub(r"</div>\s*<div[^>]*>", "\n", raw, flags=re.IGNORECASE)
    # Remove surrounding HTML head/body if present but keep inner content
    # We'll focus on content inside <body> if present
    body_m = re.search(r"<body[^>]*>(.*)</body>", raw, re.DOTALL | re.IGNORECASE)
    if body_m:
        raw = body_m.group(1)
    # Unescape entities first
    raw = html.unescape(raw)
    # Replace spans with ANSI
    processed = replace_spans(raw)
    # Remove remaining tags but preserve whitespace/newlines
    processed = re.sub(r"<[^>]+>", "", processed)
    # Convert non-breaking spaces to regular spaces
    processed = processed.replace("\u00a0", " ")
    return processed


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT
    if not path.exists():
        print(f"File not found: {path}")
        sys.exit(2)
    out = html_to_ansi(path)
    # Print with no extra resets at end (we reset inside spans)
    sys.stdout.write(out)


if __name__ == "__main__":
    main()

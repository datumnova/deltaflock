Renderer for colored HTML ASCII art

This small script converts simple HTML with inline color styles (e.g. <span style="color:#RRGGBB">) into ANSI escape codes so the art displays with colors in a terminal.

Usage

From the workspace root:

```bash
# Default (bundled example HTML)
python3 scripts/render_ascii.py

# Or pass a path to an HTML file
python3 scripts/render_ascii.py /absolute/path/to/file.html
```

Notes

- Terminal must support truecolor (24-bit) for best results. Most modern terminals (iTerm2, Alacritty, recent GNOME Terminal, Windows Terminal) do.
- The renderer handles <span style="color:..."> and basic <br> tags, plus <font color="..."> and rgb() colors. It strips other HTML tags while preserving whitespace and newlines.
- If the script can't find the file, pass an absolute path or run it from the repo root.

If you'd like, I can run the script here and show the colored output in the terminal; let me know and I'll execute it (or you can run the command above).

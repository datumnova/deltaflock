"""Application package initialiser.

Expose a callable `main()` so packaging entrypoints that do
`from src.app import main` will get a function (not a module) and can call it.
"""

from importlib import import_module

# Lazy-import the main module and delegate a callable entrypoint to it
_main_mod = import_module(".main", package=__name__)


def main(*args, **kwargs):
    """Start the application (delegates to `src.app.main.main`)."""
    return _main_mod.main(*args, **kwargs)

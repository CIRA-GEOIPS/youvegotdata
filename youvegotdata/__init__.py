import logging

try:
    import importlib.metadata

    __version__ = importlib.metadata.version(__package__ or __name__)
except Exception:
    __version__ = "unknown"
    logging.debug(
        "Could not determine __version__ - the package is not installed as a "
        "distribution (install it or run `pip install -e .`)."
    )

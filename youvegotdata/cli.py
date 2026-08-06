"""Command line interface for youvegotdata.

The ``ygd`` console script (see ``[project.scripts]`` in ``pyproject.toml``)
calls :func:`main`, which parses arguments, configures logging, loads the
config, and publishes the notification. :func:`main` returns an exit code so
it is testable without invoking ``sys.exit``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import List, Optional, Sequence

from .config import APP_NAME, ConfigError, load_config
from .notify import Notification, send_notification

log = logging.getLogger(__name__)

DESCRIPTION = """
Allows a data ingest process to send a new file notification to the Data
Inventory RabbitMQ server. The notification will ultimately be used to add the
file metadata to the Data Inventory DB.
"""

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"


def build_parser() -> argparse.ArgumentParser:
    """Build the ``ygd`` argument parser.

    Returns
    -------
    argparse.ArgumentParser
        The configured parser. Exposed separately from :func:`main` so tests
        can introspect the CLI without executing it.
    """
    parser = argparse.ArgumentParser(prog="ygd", description=DESCRIPTION.strip())

    # The positional filepath argument.
    parser.add_argument(
        "filepath",
        type=str,
        help="Send a notification for the file with this path.",
    )

    # The flags.
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output - set log level to DEBUG",
    )
    parser.add_argument(
        "-p",
        "--product",
        default=None,
        help="The file's product",
    )
    parser.add_argument(
        "-r",
        "--version",
        default=None,
        help="The file's version",
    )
    parser.add_argument(
        "-s",
        "--start_time",
        default=None,
        help="The first date and time for which the file has data",
    )
    parser.add_argument(
        "-e",
        "--end_time",
        default=None,
        help="The last date and time for which the file has data",
    )
    parser.add_argument(
        "-l", "--length", default=None, help="The length(size) of the file"
    )
    parser.add_argument("-c", "--checksum", default=None, help="The file's checksum")
    parser.add_argument(
        "-t",
        "--checksum_type",
        default=None,
        help="The type of the checksum - its algorithm",
    )
    return parser


def setup_logging(verbose: bool) -> None:
    """Configure the root logging for the CLI run.

    Parameters
    ----------
    verbose : bool
        If ``True``, set the level to DEBUG; otherwise INFO.
    """
    logging.basicConfig(
        format=LOG_FORMAT,
        level="DEBUG" if verbose else "INFO",
    )
    # Reduce pika's chatter.
    logging.getLogger("pika").setLevel(logging.WARNING)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point.

    Parameters
    ----------
    argv : sequence of str, optional
        Command line arguments (without the program name). Defaults to
        ``sys.argv[1:]``.

    Returns
    -------
    int
        Exit code: ``0`` on success, ``1`` on any failure. ``--help`` /
        invalid arguments raise ``SystemExit`` via argparse, as usual for a
        CLI.
    """
    parser = build_parser()
    pargs = parser.parse_args(argv)

    setup_logging(verbose=pargs.verbose)
    log.info("youvegotdata CLI starting (app: %s)", APP_NAME)
    log.debug("Arguments: %s", vars(pargs))

    # Read and validate the configuration file.
    try:
        config = load_config()
    except ConfigError as exc:
        log.error("%s", exc)
        return 1

    # The filepath file must exist on the local machine.
    resolved_path = os.path.realpath(pargs.filepath)
    if not os.path.exists(resolved_path):
        log.error(
            "The file %s does not exist. No notification was sent.",
            pargs.filepath,
        )
        return 1
    log.debug("Resolved filepath: %s", resolved_path)

    notification = Notification(
        filepath=pargs.filepath,
        product=pargs.product,
        version=pargs.version,
        start_time=pargs.start_time,
        end_time=pargs.end_time,
        length=pargs.length,
        checksum=pargs.checksum,
        checksum_type=pargs.checksum_type,
    )

    sent = send_notification(notification, config)
    return 0 if sent else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

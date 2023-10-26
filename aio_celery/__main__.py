import argparse
import asyncio

from . import __version__
from .worker import run


def main() -> None:
    """
    Main entrypoint of the taskiq.
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "CLI for %(prog)s\n\n"
            "Examples\n"
            "--------\n"
            "$ %(prog)s --app=proj:app worker\n"
            "$ %(prog)s -A proj:app worker -Q hipri,lopri\n"
            "$ %(prog)s -A proj:app worker --concurrency=50000\n"
        ),
    )
    parser.add_argument(
        "-A",
        "--app",
        help=(
            "Where to search for application instance. "
            "This string must be specified in 'module.module:variable' format."
        ),
    )
    parser.add_argument(
        "-V",
        "--version",
        dest="version",
        action="store_true",
        help="print current %(prog)s version and exit",
    )
    subparsers = parser.add_subparsers(
        title="Available subcommands",
        metavar="",
        dest="subcommand",
    )
    worker = subparsers.add_parser(
        "worker",
        help="Run worker",
        description="Run worker",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    worker.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=10_000,
        help="Maximum simultaneous async tasks",
    )
    worker.add_argument(
        "-Q",
        "--queues",
        help="Comma separated list of queues",
    )

    args = parser.parse_args()

    if args.version:
        print(__version__)
        return

    if args.subcommand is None:
        parser.print_help()
        return

    if args.app is None:
        parser.print_help()
        return

    asyncio.run(run(args))


if __name__ == "__main__":
    main()

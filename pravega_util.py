"""pravega_util - useful cli tools for managing streams and scope"""

import sys
import argparse
import json
import logging
import cgitb

from io.pravega.client.stream.impl import JavaSerializer

from pravega_interface import (
    streamManager,
    keyValueTableManager,
)

from util import setup_logging, add_logging_argument

def get_argument_parser():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-u",
        "--uri",
        default="tcp://127.0.0.1:9090",
        help="Pravega URI (tcp://127.0.0.1:9090)",
    )

    parser.add_argument("--scope", help="scope")

    parser.add_argument(
        "-p",
        "--purge_scope",
        help="delete all streams and kvt from scope",
        action="store_true",
        default=False,
    )
    return parser

def purge_scope(uri, scope):
    """delete all kvt and streams in specified scope"""
    with keyValueTableManager(uri) as kvt_manager:
        for kvt_info in kvt_manager.listKeyValueTables(scope):
            kvt_name = kvt_info.getKeyValueTableName()
            logging.debug("deleting kvt %s/%s", scope, kvt_name)
            kvt_manager.deleteKeyValueTable(scope, kvt_name)

    with streamManager(uri=uri) as stream_manager:
        logging.debug("deleting scope %s", scope)
        stream_manager.deleteScope(scope, True)

def main():
    """main"""
    parser = get_argument_parser()
    add_logging_argument(parser)
    args = parser.parse_args()
    setup_logging(args)

    if all((args.purge_scope, args.uri, args.scope)):
        return purge_scope(uri=args.uri, scope=args.scope)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

"""Kvt_test - just testing how kvt works"""

import sys
import argparse
import json
import logging
import cgitb

from io.pravega.client.stream.impl import JavaSerializer
from io.pravega.client.stream.impl import UTF8StringSerializer

from pravega_interface import (
    keyValueTable,
    keyValueTableFactory,
    keyValueTableConfiguration,
    keyValueTableManager,
)

from util import setup_logging, add_logging_argument

cgitb.enable(format="text")

missing = object()


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
        "-t", "--table_name", help="table name", default=None,
    )

    parser.add_argument(
        "-k", "--key_name", help="key name", default=None,
    )

    parser.add_argument(
        "-v",
        "--value",
        default=missing,
        help="write this value to table if present, otherwise read table",
    )

    return parser


def read_or_write_kvt(uri, scope, table_name, key_name, value=missing):
    # key_serializer = JavaSerializer() # this doesn't work, raises java.io.StreamCorruptedException:
    key_serializer = UTF8StringSerializer()
    # value_serializer = JavaSerializer() # this doesn't work
    value_serializer = UTF8StringSerializer()
    with keyValueTableManager(uri) as kvt_manager:
        key_value_table_configuration = keyValueTableConfiguration()
        created = kvt_manager.createKeyValueTable(
            scope, table_name, key_value_table_configuration
        )
        logging.debug(
            "kvt table %s/%s %s",
            scope,
            table_name,
            "created" if created else "already exists",
        )
        with keyValueTableFactory(uri, scope) as kvt_factory:
            with keyValueTable(
                kvt_factory, table_name, key_serializer, value_serializer
            ) as kvt_table:
                if value is missing:
                    return kvt_table.get(None, key_name).join().getValue()
                else:
                    kvt_table.put(None, key_name, value).join()


def main():
    """main"""
    parser = get_argument_parser()
    add_logging_argument(parser)
    args = parser.parse_args()
    setup_logging(args)

    if all((args.uri, args.scope, args.table_name, args.key_name)):
        # read or write to a kvt
        value = read_or_write_kvt(
            uri=args.uri,
            scope=args.scope,
            table_name=args.table_name,
            key_name=args.key_name,
            value=args.value,
        )
        print("%r" % value)
        return 0
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

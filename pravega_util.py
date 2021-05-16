"""pravega_util - useful cli tools for managing streams and scope"""

import sys
import argparse
import logging

from java.util.concurrent import CompletionException

from pravega_interface import (
    streamManager,
    keyValueTableManager,
)

from const import ALL_REDIS_KEYS

from util import setup_logging, add_logging_argument
from redis_util import add_redis_argparse_argument, get_redis_server_from_options

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


    parser.add_argument(
        "-c",
        "--purge_redis",
        help="delete keys from redis",
        action="store_true",
        default=False,
    )    
    return parser

def purge_scope(uri, scope):
    """delete all kvt and streams in specified scope"""
    try:
        with keyValueTableManager(uri) as kvt_manager:
            for kvt_info in kvt_manager.listKeyValueTables(scope):
                kvt_name = kvt_info.getKeyValueTableName()
                logging.debug("deleting kvt %s/%s", scope, kvt_name)
                kvt_manager.deleteKeyValueTable(scope, kvt_name)

        with streamManager(uri=uri) as stream_manager:
            logging.debug("deleting scope %s", scope)
            stream_manager.deleteScope(scope, True)
    except CompletionException:
        # looks like CompletionException contains NoSuchScopeException
        # will have to figure out later how to directly catch NoSuchScopeException
        pass

def purge_redis(redis):
    """clear redis data structures"""
    for redis_key_name in ALL_REDIS_KEYS:
        redis.del(redis_key_name)
        logging.debug("deleted key %r from redis", redis_key_name)


def main():
    """main"""
    parser = get_argument_parser()
    add_logging_argument(parser)
    add_redis_argparse_argument(parser)
    args = parser.parse_args()
    setup_logging(args)

    handled_params = False
    if all((args.purge_scope, args.uri, args.scope)):
        purge_scope(uri=args.uri, scope=args.scope)
        handled_params = True
        
    if args.purge_redis and args.redis_server:
        purge_redis(get_redis_server_from_options(args))
        handled_params = True

    if not handled_params:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

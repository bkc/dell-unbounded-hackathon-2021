"""import_events - import events from json file into pravega"""

import sys
import argparse
import json
import logging
import cgitb


from io.pravega.client.stream.impl import UTF8StringSerializer

from pravega_interface import (
    streamConfiguration,
    streamManager,
    eventStreamClientFactory,
    eventWriter,
)

from util import setup_logging, add_logging_argument
from const import SORTING_CENTER_TO_STREAM_NAME, SORTING_CENTER_CODES
from pravega_util import purge_scope, purge_redis
from redis_util import add_redis_argparse_argument, get_redis_server_from_options

cgitb.enable(format="text")


def import_events(uri, scope, input_file):
    """import stream of events into per sorting-center streams"""
    serializer = UTF8StringSerializer()
    with streamManager(uri=uri) as stream_manager:
        stream_manager.createScope(scope)
        with eventStreamClientFactory(uri, scope) as event_stream_client_factory:
            # ensure destination streams have already been created
            create_streams(stream_manager, scope)
            # ugly python to get context manager to work properly
            # since contextlib.nested is deprecated
            with eventWriter(
                event_stream_client_factory,
                SORTING_CENTER_TO_STREAM_NAME["A"],
                serializer,
            ) as stream_A, eventWriter(
                event_stream_client_factory,
                SORTING_CENTER_TO_STREAM_NAME["B"],
                serializer,
            ) as stream_B, eventWriter(
                event_stream_client_factory,
                SORTING_CENTER_TO_STREAM_NAME["C"],
                serializer,
            ) as stream_C, eventWriter(
                event_stream_client_factory,
                SORTING_CENTER_TO_STREAM_NAME["D"],
                serializer,
            ) as stream_D:
                sorting_center_to_stream_map = {
                    "A": stream_A,
                    "B": stream_B,
                    "C": stream_C,
                    "D": stream_D,
                }
                last_event_time = write_to_streams(
                    input_file, sorting_center_to_stream_map
                )

                # write a end of stream markers
                last_event_time = last_event_time + 84600  # one day
                for sorting_center_code in SORTING_CENTER_CODES:
                    event = {
                        "event_time": last_event_time,
                        "sorting_center": sorting_center_code,
                        "scanner_id": "end-of-stream",
                        "package_id": "none",
                    }
                    stream = sorting_center_to_stream_map[sorting_center_code]
                    stream.writeEvent(event["package_id"], json.dumps(event))


def write_to_streams(input_file, sorting_center_to_stream_map):
    """parse json input file line-by-line, route to correct stream"""
    last_event_time = None
    while 1:
        line = input_file.readline()
        if not line:
            return last_event_time

        event = json.loads(line)
        stream = sorting_center_to_stream_map[event["sorting_center"]]
        last_event_time = int(event["event_time"])
        stream.noteTime(last_event_time)  # this turned out to not be useful
        stream.writeEvent(
            event["package_id"], json.dumps(event)
        )  # this appears to serialize to a rather large amount of data


def create_streams(stream_manager, scope):
    """create input streams as needed"""
    stream_configuration = streamConfiguration(
        scaling_policy=1
    )  # might need to use different policy
    for stream_name in SORTING_CENTER_TO_STREAM_NAME.values():
        created = stream_manager.createStream(scope, stream_name, stream_configuration)
        logging.debug(
            "stream %s/%s %s",
            scope,
            stream_name,
            "created" if created else "already exists",
        )


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
        "-i", "--import_file", help="json file to import (- = stdin)", default=None,
    )

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


def main():
    """main"""
    parser = get_argument_parser()
    add_logging_argument(parser)
    add_redis_argparse_argument(parser)
    args = parser.parse_args()
    setup_logging(args)

    if all((args.purge_scope, args.uri, args.scope)):
        purge_scope(uri=args.uri, scope=args.scope)

    if args.purge_redis and args.redis_server:
        purge_redis(get_redis_server_from_options(args))

    if args.import_file:
        # import events from file
        if args.import_file == "-":
            input_file = sys.stdin
        else:
            input_file = open(args.import_file, "r")

        import_events(uri=args.uri, scope=args.scope, input_file=input_file)
        return 0
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

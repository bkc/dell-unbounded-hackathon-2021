"""import_events - import events from json file into pravega"""

import sys
import argparse
import json
import logging
import cgitb

from io.pravega.client.stream.impl import JavaSerializer

from pravega_interface import (
    streamConfiguration,
    streamManager,
    eventStreamClientFactory,
    eventWriter,
)

from util import setup_logging, add_logging_argument

SORTING_CENTER_TO_STREAM_NAME = {_: "sorting-center-input-%s" % _ for _ in "ABCD"}

cgitb.enable(format="text")


def import_events(uri, scope, input_file):
    """import stream of events into per sorting-center streams"""
    serializer = JavaSerializer()
    with streamManager(uri=uri) as stream_manager, eventStreamClientFactory(
        uri, scope
    ) as event_stream_client_factory:
        # ensure destination streams are written
        create_streams(stream_manager, scope)
        # ugly python to get context manager to work properly
        # since contextlib.nested is deprecated
        with eventWriter(
            event_stream_client_factory, SORTING_CENTER_TO_STREAM_NAME["A"], serializer
        ) as stream_A, eventWriter(
            event_stream_client_factory, SORTING_CENTER_TO_STREAM_NAME["B"], serializer
        ) as stream_B, eventWriter(
            event_stream_client_factory, SORTING_CENTER_TO_STREAM_NAME["C"], serializer
        ) as stream_C, eventWriter(
            event_stream_client_factory, SORTING_CENTER_TO_STREAM_NAME["D"], serializer
        ) as stream_D:
            sorting_center_to_stream_map = {
                "A": stream_A,
                "B": stream_B,
                "C": stream_C,
                "D": stream_D,
            }
            write_to_streams(input_file, sorting_center_to_stream_map)


def write_to_streams(input_file, sorting_center_to_stream_map):
    """parse json input file line-by-line, route to correct stream"""
    while 1:
        line = input_file.readline()
        if not line:
            return

        event = json.loads(line)
        logging.debug("%r", event)
        stream = sorting_center_to_stream_map[event["sorting_center"]]
        stream.noteTime(int(event["event_time"]))
        stream.writeEvent(
            event["package_id"], event
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

    return parser


def main():
    """main"""
    parser = get_argument_parser()
    add_logging_argument(parser)
    args = parser.parse_args()
    setup_logging(args)

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

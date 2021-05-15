"""sorting_center - process all tracking events for an individual sorting center"""

"""import_events - import events from json file into pravega"""

import sys
import argparse
import json
import logging
import cgitb
import itertools
import uuid

from io.pravega.client.stream.impl import JavaSerializer
from io.pravega.client.stream import Stream

from pravega_interface import (
    streamConfiguration,
    streamManager,
    eventStreamClientFactory,
    eventWriter,
    readerGroupManager,
    readerGroup,
    Reader,
)

from util import setup_logging, add_logging_argument
from const import SORTING_CENTER_CODES, SORTING_CENTER_TO_STREAM_NAME

READ_TIMEOUT = 2000

cgitb.enable(format="text")


def import_events(uri, scope, input_file):
    """import stream of events into per sorting-center streams"""
    serializer = JavaSerializer()
    with streamManager(uri=uri) as stream_manager, eventStreamClientFactory(
        uri, scope
    ) as event_stream_client_factory:
        # ensure destination streams have already been created
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


def iterable_stream(uri, scope, stream_name, serializer, reader_name=None):
    """iterate events from a stream"""
    if reader_name is None:
        reader_name = str(uuid.uuid4()).replace("-", "")
    with readerGroupManager(uri, scope) as reader_group_manager, readerGroup(
        reader_group_manager, scope, stream_name
    ) as reader_group, eventStreamClientFactory(uri, scope) as client_factory, Reader(
        reader_group, client_factory, serializer, reader_name=reader_name
    ) as reader:
        while True:
            event_read = reader.readNextEvent(READ_TIMEOUT)
            event = event_read.getEvent()
            if event is None:
                if reader_group.getMetrics().unreadBytes():
                    # still more to read, retry
                    continue
                else:
                    # nothing left to read
                    return

            yield event_read


def process_sorting_center_events(uri, scope, sorting_center_code):
    """process events from stream"""
    serializer = JavaSerializer()
    with streamManager(uri=uri) as stream_manager:
        # input stream must already exist
        input_stream_name = SORTING_CENTER_TO_STREAM_NAME[sorting_center_code]
        logging.debug("begin reading from stream %r", input_stream_name)
        input_event_stream = iterable_stream(uri, scope, input_stream_name, serializer)
        pipeline = save_streamcut_timestamps(input_event_stream)

        # read each scan event
        # write hourly window times back to sorting-center specific timestamp stream
        # always update redis sorted set with next expected  event time

        # if its from intake scanner - send package_id, destination, eta and value to central service via kvt
        # if its weighing scanner - update central service kvt, add weight
        # if its intake, holding, receiving or outlet - add event to package specific stream

        for _ in itertools.izip(range(10), pipeline):
            logging.debug("%r", _)
    return 0


def save_streamcut_timestamps(input_event_stream):
    """save streamcuts every hour somewhere so we can rewind the stream"""
    # to make it more efficient to extract events for a specific package_id
    # for when we want to extract the tracking details on a package
    # we can rewind to the probable location of the first tracking event
    # then read forward.
    # due to short-time on the hackathon, I'm not going to actually implement this
    # to rewind a stream, we need a StreamCut, which can be created by
    # reader_group.generateStreamCuts  We can then save the streamCut by serializing it
    # posting back to another stream or kvt or wherever.
    for event_read in input_event_stream:
        # logging.debug("%r %r", currentTimeWindow, event_read)
        yield event_read


def extract_sorting_center_events_by_package_id(
    uri, scope, sorting_center_code, package_id
):
    """yield events for only this package_id"""
    serializer = JavaSerializer()
    with streamManager(uri=uri) as stream_manager:
        # input stream must already exist
        input_stream_name = SORTING_CENTER_TO_STREAM_NAME[sorting_center_code]
        logging.debug("begin reading from stream %r", input_stream_name)
        input_event_stream = iterable_stream(uri, scope, input_stream_name, serializer)
        for event in filter_events_by_package_id(input_event_stream, package_id):
            yield event


def filter_events_by_package_id(input_stream, package_id):
    """yield events for the requested package_id"""
    for read_event in input_stream:
        event = read_event.getEvent()
        if event.get("package_id") != package_id:
            continue
        yield read_event


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
        "-p", "--package_id", help="extract events for only this package (testing only)"
    )

    parser.add_argument(
        "-s",
        "--sorting_center_code",
        choices=SORTING_CENTER_CODES,
        help="the sorting center code (one of %r)" % SORTING_CENTER_CODES,
        default=None,
    )

    parser.add_argument(
        "-r",
        "--run",
        help="run sorting center process",
        action="store_true",
        default=False,
    )
    return parser


def main():
    """main"""
    parser = get_argument_parser()
    add_logging_argument(parser)
    args = parser.parse_args()
    setup_logging(args)

    if all((args.sorting_center_code, args.scope, args.uri, args.run)):
        # run the sorting center process
        return process_sorting_center_events(
            uri=args.uri, scope=args.scope, sorting_center_code=args.sorting_center_code
        )
    elif all((args.sorting_center_code, args.scope, args.uri, args.package_id)):
        # test retrieving events for a single package
        for event in extract_sorting_center_events_by_package_id(
            uri=args.uri,
            scope=args.scope,
            sorting_center_code=args.sorting_center_code,
            package_id=args.package_id,
        ):
            print("%r" % event.getEvent())

    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

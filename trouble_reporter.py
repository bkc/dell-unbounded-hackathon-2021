"""trouble_report - tail trouble log and report on lost and late packages"""

import sys
import argparse
import json
import logging
import cgitb
import itertools
import uuid
import operator
import time
import datetime

from io.pravega.client.stream.impl import JavaSerializer
from io.pravega.client.stream.impl import UTF8StringSerializer

from pravega_interface import (
    streamConfiguration,
    streamManager,
    eventStreamClientFactory,
    eventWriter,
    readerGroupManager,
    readerGroup,
    Reader,
    keyValueTable,
    keyValueTableFactory,
    keyValueTableConfiguration,
    keyValueTableManager,
)

from redis_util import add_redis_argparse_argument, get_redis_server_from_options

from util import setup_logging, add_logging_argument
from const import (
    SORTING_CENTER_CODES,
    SORTING_CENTER_TO_STREAM_NAME,
    REDIS_PACKAGE_NEXT_EVENT_KEY_NAME,
    REDIS_CLOCK_SYNC_KEY_NAME,
    PACKAGE_ATTRIBUTES_KVT_NAME,
    PACKAGE_EVENTS_KVT_NAME,
    PUBLIC_SCANNER_EVENTS,
    TROUBLE_EVENT_STREAM_NAME,
    MINIMUM_LATE_PACKAGE_SECONDS,
    REDIS_LATE_PACKAGE_HASH_NAME,
    REDIS_PACKAGE_NEXT_SCANNER_ID_KEY_NAME,
)

READ_TIMEOUT = 2000


cgitb.enable(format="text")
logger = None


def iterable_stream(
    uri, scope, stream_name, serializer, reader_name=None, wait_for_events=False
):
    """iterate events from a stream"""
    if reader_name is None:
        reader_name = str(uuid.uuid4()).replace("-", "")
    with readerGroupManager(uri, scope) as reader_group_manager, readerGroup(
        reader_group_manager, scope, stream_name
    ) as reader_group, eventStreamClientFactory(uri, scope) as client_factory, Reader(
        reader_group, client_factory, serializer, reader_name=reader_name
    ) as reader:
        have_read_an_event = False
        while True:
            event_read = reader.readNextEvent(READ_TIMEOUT)
            event = event_read.getEvent()
            if event is None:
                if reader_group.getMetrics().unreadBytes():
                    # still more to read, retry
                    continue
                elif not have_read_an_event and wait_for_events:
                    # need to keep retrying until we get at least one event
                    logger.debug("waiting for events")
                    continue
                else:
                    # nothing left to read
                    logger.debug("all events have been read")
                    return

            yield event
            have_read_an_event = True


def report_trouble_events(
    uri, scope, redis=None, wait_for_events=False,
):
    """process events from trouble stream"""
    serializer = JavaSerializer()
    kvt_serializer = (
        UTF8StringSerializer()
    )  # cannot get kvt to work with JavaSerializer
    package_attribute_kvt_table_name = PACKAGE_ATTRIBUTES_KVT_NAME
    key_value_table_configuration = keyValueTableConfiguration()
    trouble_stream_name = TROUBLE_EVENT_STREAM_NAME
    stream_configuration = streamConfiguration(scaling_policy=1)
    with streamManager(uri=uri) as stream_manager:
        stream_manager.createScope(scope)
        with keyValueTableManager(uri) as kvt_manager:
            created = kvt_manager.createKeyValueTable(
                scope, package_attribute_kvt_table_name, key_value_table_configuration
            )
            logger.debug(
                "kvt table %s/%s %s",
                scope,
                package_attribute_kvt_table_name,
                "created" if created else "already exists",
            )

            created = stream_manager.createStream(
                scope, trouble_stream_name, stream_configuration
            )
            logger.debug(
                "stream %s/%s %s",
                scope,
                trouble_stream_name,
                "created" if created else "already exists",
            )
            with keyValueTableFactory(uri, scope) as kvt_factory:
                with keyValueTable(
                    kvt_factory,
                    package_attribute_kvt_table_name,
                    kvt_serializer,
                    kvt_serializer,
                ) as package_attribute_kvt_table:
                    logger.debug("begin reading from stream %r", trouble_stream_name)
                    input_event_stream = iterable_stream(
                        uri,
                        scope,
                        trouble_stream_name,
                        serializer,
                        wait_for_events=wait_for_events,
                    )

                    # process all events by completely consuming the generator
                    for event in input_event_stream:
                        package_id = event["package_id"]
                        kvt_entry = package_attribute_kvt_table.get(
                            None, package_id
                        ).join()
                        package_attributes = (
                            json.loads(kvt_entry.getValue()) if kvt_entry else {}
                        )
                        yield (event, package_attributes)


def report_events(trouble_events):
    """format and output trouble-events"""
    for event, package_attributes in trouble_events:
        event_type = event["event_type"]
        at_time = datetime.datetime.fromtimestamp(event["event_time"]).strftime(
            "%m-%d %H:%M"
        )
        package_info = (
            "pkg %-5.5s weight %-2.2s value $%s origin %s dest %s est. del %s"
            % (
                event["package_id"],
                package_attributes.get("weight", "?"),
                package_attributes.get("declared_value", "?"),
                package_attributes.get("origin"),
                package_attributes.get("destination"),
                datetime.datetime.fromtimestamp(
                    package_attributes["estimated_delivery_time"]
                ).strftime("%m-%d %H:%M"),
            )
        )
        if event_type == "late_delivery":
            logger.info("at %s late  %s", at_time, package_info)
        elif event_type == "lost_package":
            logger.info("at %s LOST  %s", at_time, package_info)
        elif event_type == "delayed_package":
            logger.info(
                "at %s delay %s before %s",
                at_time,
                package_info,
                event["next_scanner_id"],
            )
        yield event, package_attributes


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
        "-r",
        "--run",
        help="run trouble reporter process",
        action="store_true",
        default=False,
    )

    parser.add_argument(
        "-w",
        "--wait_for_events",
        help="wait for at least one event before exiting",
        action="store_true",
        default=False,
    )

    return parser


def main():
    """main"""
    global logger
    parser = get_argument_parser()
    add_logging_argument(parser)
    add_redis_argparse_argument(parser)
    args = parser.parse_args()
    setup_logging(args)
    logger = logging.getLogger("Report")

    if all((args.scope, args.uri, args.run)):
        # run the sorting center process
        redis = get_redis_server_from_options(args)
        for _ in report_events(
            report_trouble_events(
                uri=args.uri,
                scope=args.scope,
                redis=redis,
                wait_for_events=args.wait_for_events,
            )
        ):
            pass

    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

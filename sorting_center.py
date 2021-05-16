"""sorting_center - process all tracking events for an individual sorting center"""

"""import_events - import events from json file into pravega"""

import sys
import argparse
import json
import logging
import cgitb
import itertools
import uuid
import operator
import time

from io.pravega.client.stream.impl import JavaSerializer
from io.pravega.client.stream.impl import UTF8StringSerializer
from io.pravega.client.stream import Stream

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
)

READ_TIMEOUT = 500
DELAYED_PACKAGE_EVENT_CHECK_FREQUENCY = (
    60  # how many seconds (simulated time) between checking for delayed events
)
SLEEP_THIS_PROCESS_WHEN_TIME_SYNC_DIFFERENCE_EXCEEDS = 90
SLEEP_PROCESS_TIME = 0.001
DEBUG_TIME_SYNC = False

cgitb.enable(format="text")
logger = None


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
        logger.debug(
            "stream %s/%s %s",
            scope,
            stream_name,
            "created" if created else "already exists",
        )


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


def process_sorting_center_events(
    uri,
    scope,
    sorting_center_code,
    redis=None,
    maximum_event_count=None,
    wait_for_events=False,
):
    """process events from stream"""
    serializer = JavaSerializer()
    with streamManager(uri=uri) as stream_manager:
        # input stream must already exist
        input_stream_name = SORTING_CENTER_TO_STREAM_NAME[sorting_center_code]
        logger.debug("begin reading from stream %r", input_stream_name)
        input_event_stream = iterable_stream(
            uri, scope, input_stream_name, serializer, wait_for_events=wait_for_events
        )
        # read each scan event
        # write hourly window times back to sorting-center specific timestamp stream
        # always update redis sorted set with next expected  event time
        # detect packages that are late and report them to trouble stream
        # if its from intake scanner - send package_id, destination, eta and value to central service via kvt
        # if its from output scanner - mark package as delivered in kvt
        # if its weighing scanner - update central service kvt, add weight
        # if its intake, holding, receiving or outlet - add event to package specific stream

        pipeline = detect_delayed_packages(
            input_event_stream=update_next_event_time(
                input_event_stream=record_public_tracking_events(
                    input_event_stream=record_intake_and_weight_and_output(
                        input_event_stream=save_streamcut_timestamps(
                            input_event_stream
                        ),
                        uri=uri,
                        scope=scope,
                    ),
                    uri=uri,
                    scope=scope,
                ),
                redis=redis,
            ),
            stream_manager=stream_manager,
            redis=redis,
            uri=uri,
            scope=scope,
            sorting_center_code=sorting_center_code,
        )

        if maximum_event_count:
            for _ in itertools.izip(range(10), pipeline):
                logger.debug("%r", _)
        else:
            # process all events by completely consuming the generator
            for idx, _ in enumerate(pipeline):
                if idx and not (idx % 100):
                    logger.debug("event # %d", idx)

    return 0


def update_next_event_time(input_event_stream, redis=None):
    """save next expected event time into redis"""
    if not redis:
        for event in input_event_stream:
            # yield from not supported in jython
            yield event
        return

    for event in input_event_stream:
        package_id = event["package_id"]
        next_event_time = event.get("next_event_time")

        if next_event_time:
            # insert member with next_event_time as score
            redis.zadd(REDIS_PACKAGE_NEXT_EVENT_KEY_NAME, next_event_time, package_id)
            redis.srem(REDIS_LATE_PACKAGE_HASH_NAME, package_id)
        else:
            # remove member
            redis.zrem(REDIS_PACKAGE_NEXT_EVENT_KEY_NAME, package_id)
            redis.srem(REDIS_LATE_PACKAGE_HASH_NAME, package_id)

        yield event


def detect_delayed_packages(
    input_event_stream, stream_manager, redis, uri, scope, sorting_center_code
):
    """check redis for delayed events, report them to another stream"""
    serializer = JavaSerializer()
    stream_configuration = streamConfiguration(scaling_policy=1)
    stream_name = TROUBLE_EVENT_STREAM_NAME
    created = stream_manager.createStream(scope, stream_name, stream_configuration)
    logger.debug(
        "stream %s/%s %s",
        scope,
        stream_name,
        "created" if created else "already exists",
    )

    with eventStreamClientFactory(uri, scope) as event_stream_client_factory:
        with eventWriter(
            event_stream_client_factory, stream_name, serializer
        ) as trouble_stream:
            last_event_seconds = 0
            for event in input_event_stream:
                event_time = event["event_time"]
                yield event
                if not last_event_seconds:
                    last_event_seconds = divmod(
                        event_time, DELAYED_PACKAGE_EVENT_CHECK_FREQUENCY
                    )
                else:
                    if last_event_seconds != divmod(
                        event_time, DELAYED_PACKAGE_EVENT_CHECK_FREQUENCY
                    ):
                        last_event_seconds = divmod(
                            event_time, DELAYED_PACKAGE_EVENT_CHECK_FREQUENCY
                        )
                        report_delayed_packages(
                            redis, trouble_stream, event_time, sorting_center_code
                        )


def report_delayed_packages(redis, stream, event_time, sorting_center_code):
    """ask redis for package ids whose next event should have occurred by now"""
    packages_to_remove = []

    # simulation requires the 'earliest current event time' to be synchronized
    # among all the sorting centers, otherwise one process runs ahead of the others
    # it will detect delayed packages that haven't had a chance to be processed
    # in other sorting centers

    # vote on current time
    redis.zadd(REDIS_CLOCK_SYNC_KEY_NAME, event_time, sorting_center_code)
    earlier_event_times = list(
        redis.zrangeByScoreWithScores(REDIS_CLOCK_SYNC_KEY_NAME, 0, event_time)
    )
    if earlier_event_times:
        earliest_event_time = earlier_event_times[0]
        time_difference = event_time - int(earliest_event_time.score)
        if time_difference > SLEEP_THIS_PROCESS_WHEN_TIME_SYNC_DIFFERENCE_EXCEEDS:
            # give other processes a chance to catch up
            if DEBUG_TIME_SYNC:
                logger.debug(
                    "sort center %r is at time %r, time difference %r, sleeping",
                    earliest_event_time.element,
                    int(earliest_event_time.score),
                    time_difference,
                )
            time.sleep(SLEEP_PROCESS_TIME)
        event_time = int(earliest_event_time.score)

    for late_package_info in list(
        redis.zrangeByScoreWithScores(REDIS_PACKAGE_NEXT_EVENT_KEY_NAME, 0, event_time)
    ):
        package_id = late_package_info.element
        expected_event_time = int(late_package_info.score)
        if event_time - expected_event_time < MINIMUM_LATE_PACKAGE_SECONDS:
            # not actually late yet
            continue

        if redis.sadd(REDIS_LATE_PACKAGE_HASH_NAME, package_id):
            logger.warn(
                "late package %r expected_event_time %r current_time %r diff %r",
                package_id,
                expected_event_time,
                event_time,
                event_time - expected_event_time,
            )

            # write to trouble stream
            # prepare to remove

            packages_to_remove.append(package_id)

    if packages_to_remove:
        redis.zrem(REDIS_PACKAGE_NEXT_EVENT_KEY_NAME, packages_to_remove)


def save_streamcut_timestamps(input_event_stream):
    """save streamcuts every hour somewhere so we can rewind the stream"""
    # to make it more efficient to extract events for a specific package_id in the future
    # we can rewind to the probable location of the first tracking event
    # then read forward.
    # due to short-time on the hackathon, I'm not going to actually implement this
    # to rewind a stream, we need a StreamCut, which can be created by
    # reader_group.generateStreamCuts  We can then save the streamCut by serializing it
    # posting back to another stream or kvt or wherever.
    for event_read in input_event_stream:
        # when event time rolls over to the 'next' hour
        # create a StreamCut and save it for future use
        yield event_read


def record_intake_and_weight_and_output(input_event_stream, uri, scope):
    """save attributes about the package in kvt table that is shared between sorting centers"""
    serializer = UTF8StringSerializer()  # cannot get kvt to work with JavaSerializer
    kvt_table_name = PACKAGE_ATTRIBUTES_KVT_NAME
    with keyValueTableManager(uri) as kvt_manager:
        key_value_table_configuration = keyValueTableConfiguration()
        created = kvt_manager.createKeyValueTable(
            scope, kvt_table_name, key_value_table_configuration
        )
        logger.debug(
            "kvt table %s/%s %s",
            scope,
            kvt_table_name,
            "created" if created else "already exists",
        )
        with keyValueTableFactory(uri, scope) as kvt_factory:
            with keyValueTable(
                kvt_factory, kvt_table_name, serializer, serializer
            ) as kvt_table:
                for event in input_event_stream:
                    scanner_id = event["scanner_id"]
                    if scanner_id not in ("intake", "weighing", "output"):
                        yield event
                        continue

                    # need to update or create kvt entry
                    package_id = event["package_id"]
                    kvt_entry = kvt_table.get(None, package_id).join()
                    value_data = json.loads(kvt_entry.getValue()) if kvt_entry else {}
                    if scanner_id == "weighing":
                        value_data["weight"] = event["weight"]
                    elif scanner_id == "output":
                        value_data["delivered_time"] = event["event_time"]
                    else:
                        value_data["intake_time"] = event["event_time"]
                        value_data["destination"] = event["destination"]
                        value_data["origin"] = event["sorting_center"]
                        value_data["declared_value"] = event["declared_value"]
                        value_data["estimated_delivery_time"] = event[
                            "estimated_delivery_time"
                        ]

                    kvt_table.put(None, package_id, json.dumps(value_data)).join()
                    yield event


def record_public_tracking_events(input_event_stream, uri, scope):
    """save public package events in kvt table that is shared between sorting centers"""
    # used to show public tracking results to customer
    serializer = UTF8StringSerializer()  # cannot get kvt to work with JavaSerializer
    kvt_table_name = PACKAGE_EVENTS_KVT_NAME
    sorted_event_key = operator.itemgetter("event_time")
    with keyValueTableManager(uri) as kvt_manager:
        key_value_table_configuration = keyValueTableConfiguration()
        created = kvt_manager.createKeyValueTable(
            scope, kvt_table_name, key_value_table_configuration
        )
        logger.debug(
            "kvt table %s/%s %s",
            scope,
            kvt_table_name,
            "created" if created else "already exists",
        )
        with keyValueTableFactory(uri, scope) as kvt_factory:
            with keyValueTable(
                kvt_factory, kvt_table_name, serializer, serializer
            ) as kvt_table:
                for event in input_event_stream:
                    scanner_id = event["scanner_id"]
                    if scanner_id not in PUBLIC_SCANNER_EVENTS:
                        yield event
                        continue

                    # need to update or create kvt entry
                    package_id = event["package_id"]
                    event_time = event["event_time"]
                    kvt_entry = kvt_table.get(None, package_id).join()
                    value_data = json.loads(kvt_entry.getValue()) if kvt_entry else []
                    event_times = [_["event_time"] for _ in value_data]
                    if event_time not in event_times:
                        # add this event to list
                        value_data.append(
                            {
                                "event_time": event_time,
                                "sorting_center": event["sorting_center"],
                                "scanner_id": event["scanner_id"],
                            }
                        )
                        kvt_table.put(
                            None,
                            package_id,
                            json.dumps(sorted(value_data, key=sorted_event_key)),
                        ).join()
                    yield event


def extract_sorting_center_events_by_package_id(
    uri, scope, sorting_center_code, package_id
):
    """yield events for only this package_id, used by cli for debugging"""
    # if we were saving StreamCuts, then we could look up the package_id from master kvt to
    # find initial import timestamp for this sorting center, then configure
    # this ReaderGroup to start at that StreamCut

    serializer = JavaSerializer()
    with streamManager(uri=uri) as stream_manager:
        # input stream must already exist
        input_stream_name = SORTING_CENTER_TO_STREAM_NAME[sorting_center_code]
        logger.debug("begin reading from stream %r", input_stream_name)
        input_event_stream = iterable_stream(uri, scope, input_stream_name, serializer)
        for event in filter_events_by_package_id(input_event_stream, package_id):
            yield event
            if event.get("scanner_id") == "output":
                # just to speed it up
                break


def filter_events_by_package_id(input_stream, package_id):
    """yield events for the requested package_id"""
    for event in input_stream:
        if event.get("package_id") != package_id:
            continue
        yield event


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
        "-m",
        "--maximum_event_count",
        type=int,
        help="maximum number of events to process (for testing)",
        default=None,
    )

    parser.add_argument(
        "-r",
        "--run",
        help="run sorting center process",
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
    if args.sorting_center_code:
        logger = logging.getLogger("Sort Center %s" % args.sorting_center_code)
    else:
        logger = logging.getLogger()

    if all((args.sorting_center_code, args.scope, args.uri, args.run)):
        # run the sorting center process
        redis = get_redis_server_from_options(args)
        return process_sorting_center_events(
            uri=args.uri,
            scope=args.scope,
            sorting_center_code=args.sorting_center_code,
            redis=redis,
            maximum_event_count=args.maximum_event_count,
            wait_for_events=args.wait_for_events,
        )
    elif all((args.sorting_center_code, args.scope, args.uri, args.package_id)):
        # test retrieving events for a single package
        for event in extract_sorting_center_events_by_package_id(
            uri=args.uri,
            scope=args.scope,
            sorting_center_code=args.sorting_center_code,
            package_id=args.package_id,
        ):
            print("%r" % event)

    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

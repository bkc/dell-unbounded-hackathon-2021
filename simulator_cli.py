"""simulator_cli - command line interface to test and operate simulator code"""
# to facilitate testing on cpython because its faster than jython
# also, not using virtualenv because jython support is poor

import sys
import argparse
import time
import logging

from util import setup_logging, add_logging_argument

from simulator_core import Simulator


def get_argument_parser():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-s",
        "--simulated_time",
        type=int,
        default=1440,
        help="total simulated time period in minutes (real-world time, e.g. 1440 = 1 day)",
    )

    parser.add_argument(
        "-r",
        "--runtime",
        type=int,
        default=60,
        help="actual simulation runtime in seconds",
    )

    parser.add_argument(
        "-p",
        "--package_count",
        type=int,
        default=1,
        help="total number of packages to be simulated",
    )

    parser.add_argument(
        "-t",
        "--test",
        dest="test_simulator",
        help="run simulation test",
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

    if args.test_simulator:
        # read the scope/stream from uri
        simulator = Simulator(
            simulated_time=args.simulated_time,
            runtime=args.runtime,
            package_count=args.package_count,
            simulated_start_time=int(time.time()),
        )
        for event in simulator.event_source():
            logging.info("%r", event)

    else:
        parser.print_help()


if __name__ == "__main__":
    sys.exit(main())

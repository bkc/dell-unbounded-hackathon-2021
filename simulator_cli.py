"""simulator_cli - command line interface to test and operate simulator code"""
# to facilitate testing on cpython because its faster than jython
# also, not using virtualenv because jython support is poor
# in the future, this cli could be changed to inject events directly into Pravega
# by using some clever in-memory sorting of event streams.

import sys
import argparse
import time
import json

from util import setup_logging, add_logging_argument

from simulator_core import Simulator


def get_argument_parser():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-s",
        "--simulated_run_time",
        type=int,
        default=1440,
        help="total simulated running time (minutes, real-world time, e.g. 1440 = 1 day)",
    )

    parser.add_argument(
        "-i",
        "--intake_run_time",
        type=int,
        default=300,
        help="total simulated running time to intake packages (minutes)",
    )

    parser.add_argument(
        "-p",
        "--package_count",
        type=int,
        default=1,
        help="total number of packages to be simulated",
    )

    parser.add_argument(
        "-d",
        "--delayed_package_count",
        type=int,
        default=0,
        help="total number of packages to be delayed",
    )

    parser.add_argument(
        "--lost_package_count",
        type=int,
        default=0,
        help="total number of packages to be lost enroute (must be less than delayed_package_count)",
    )

    parser.add_argument(
        "-t",
        "--test",
        dest="test_simulator",
        help="run simulation test",
        action="store_true",
        default=False,
    )

    parser.add_argument(
        "-j", "--json_output", help="output json", action="store_true", default=False,
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
            simulated_run_time=args.simulated_run_time,
            intake_run_time=args.intake_run_time,
            package_count=args.package_count,
            simulated_start_time=int(time.time()),
            lost_package_count=args.lost_package_count,
            delayed_package_count=args.delayed_package_count,
        )
        for event in simulator.event_source():
            if args.json_output:
                sys.stdout.write("%s\n" % json.dumps(event))
            else:
                print("%r" % event)

    else:
        parser.print_help()


if __name__ == "__main__":
    sys.exit(main())

"""simulator_core - generates package barcode scan events"""
import time
import random
import logging

from const import SORTING_CENTER_CODES


SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600

TRUCK_TRAVEL_TIMES = {
    ("A", "A"): 0,
    ("A", "B"): 1440,
    ("A", "C"): 1440 * 2,
    ("A", "D"): 1440 * 5,
    ("B", "A"): 1440,
    ("B", "B"): 0,
    ("B", "C"): 1440,
    ("B", "D"): 1440 * 5,
    ("C", "A"): 1440 * 2,
    ("C", "B"): 1440,
    ("C", "C"): 0,
    ("C", "D"): 1440 * 5,
    ("D", "A"): 1440 * 5,
    ("D", "B"): 1440 * 5,
    ("D", "C"): 1440 * 5,
    ("D", "D"): 0,
}


class SortingCenter:
    """information about sorting center scanner arrangement"""

    # travel_time is in minutes
    PATH_FROM = {
        "intake": [
            # {
            #     "next": "intake",
            #     "travel_time": random.randint(2, 5) * SECONDS_PER_MINUTE,
            # },
            {
                "next": "weighing",
                "travel_time": random.randint(2, 5) * SECONDS_PER_MINUTE,
            },
            {
                "next": "pre-routing",
                "travel_time": random.randint(2, 5) * SECONDS_PER_MINUTE,
            },
            {
                "next": "routing",
                "travel_time": random.randint(5, 10) * SECONDS_PER_MINUTE,
            },
        ],
        "receiving": [
            # {
            #     "next": "receiving",
            #     "travel_time": random.randint(2, 5) * SECONDS_PER_MINUTE,
            # },
            {
                "next": "pre-routing",
                "travel_time": random.randint(2, 5) * SECONDS_PER_MINUTE,
            },
            {
                "next": "routing",
                "travel_time": random.randint(5, 10) * SECONDS_PER_MINUTE,
            },
        ],
    }
    PATH_TO = {
        "output": [
            {
                "next": "output",
                "travel_time": random.randint(5, 15) * SECONDS_PER_MINUTE,
            },
            {"next": None, "travel_time": 0,},
        ],
        "holding": [
            {
                "next": "holding",
                "travel_time": random.randint(5, 15) * SECONDS_PER_MINUTE,
            },
        ],
    }

    INTAKE_TIME = sum(_["travel_time"] for _ in PATH_FROM["intake"])
    RECEIVING_TIME = sum(_["travel_time"] for _ in PATH_FROM["receiving"])
    OUTPUT_TIME = sum(_["travel_time"] for _ in PATH_TO["output"])
    HOLDING_TIME = sum(_["travel_time"] for _ in PATH_TO["holding"])

    def __init__(self, name="A"):
        self.name = name

    def package_path(self, origin, destination):
        """yield events between origin an destination"""
        origin_scanner = "intake" if origin == self.name else "receiving"
        for path_info in self.PATH_FROM[origin_scanner]:
            yield path_info

        if destination == self.name:
            for path_info in self.PATH_TO["output"]:
                yield path_info
        else:
            for path_info in self.PATH_TO["holding"]:
                if path_info["next"] == "holding":
                    path_info = path_info.copy()
                    path_info["next"] = "holding_%s" % destination

                yield path_info


class Simulator:
    """package barcode scan simulator

    The simulator creates a sequence of barcode scan events
    for packages. The events contain a real-world timestamp that is 
    derived from the current starting time and the simulated time

    events are emitted for runtime seconds.

    e.g. if the simulated_time is 1440 minutes and the runtime is 300 seconds
    then over the course of 5 minutes events simulating one day's worth of
    package scans will be generated
    """

    def __init__(
        self,
        simulated_run_time=1440,
        intake_run_time=300,
        package_count=10,
        simulated_start_time=0,
        delayed_package_count=0,
        lost_package_count=0,
    ):
        if not simulated_start_time:
            simulated_start_time = int(time.time())
        self.simulated_start_time = simulated_start_time
        self.simulated_end_time = (
            simulated_run_time * SECONDS_PER_MINUTE + self.simulated_start_time
        )
        self.seconds_per_package = (
            float(intake_run_time * SECONDS_PER_MINUTE) / package_count
        )  # time between package creation events
        self.package_count = package_count
        logging.debug(
            "start_time %r end_time %r duration %r package_count %r seconds_per_package %r",
            self.simulated_start_time,
            self.simulated_end_time,
            self.simulated_end_time - self.simulated_start_time,
            self.package_count,
            self.seconds_per_package,
        )
        self.sorting_centers = {_: SortingCenter(name=_) for _ in SORTING_CENTER_CODES}
        self.lost_or_delayed_package_map = self.generate_lost_or_delayed_packages(
            package_count=package_count,
            lost_package_count=lost_package_count,
            delayed_package_count=delayed_package_count,
        )

    def event_source(self):
        """yield barcode scanning events for packages"""
        # initial naive approach is to generate all events for each package
        # individually, sort all events, then inject them into Pravega

        # generated packages need to be spread out over the simulated run time
        event_time = float(self.simulated_start_time)
        for package_id in range(1, self.package_count + 1):
            package_id = str(package_id)
            lost_or_delay_info = self.lost_or_delayed_package_map.get(package_id)
            delay_offset = 0
            for event_index, event in enumerate(
                self.package_lifecycle(event_time=event_time, package_id=package_id)
            ):
                event["event_time"] += delay_offset
                if "next_event_time" in event:
                    event["next_event_time"] += delay_offset
                yield event
                if (
                    lost_or_delay_info
                    and lost_or_delay_info["event_index"] == event_index
                ):
                    logging.debug(
                        "delay or lose package_id %r before scanner_id %r type %r",
                        package_id,
                        event.get("next_scanner_id"),
                        lost_or_delay_info["type"],
                    )
                    # delay this package or lose it
                    if lost_or_delay_info["type"] == "lost":
                        # lose the package
                        break
                    # just delay it
                    delay_offset = lost_or_delay_info["delay"]
                    lost_or_delay_info = None

            event_time += self.seconds_per_package

    def package_lifecycle(self, event_time, package_id):
        """generate lifecycle of one package"""
        origin = random.choice(SORTING_CENTER_CODES)
        destination = random.choice(SORTING_CENTER_CODES)

        current_scanner = "intake"
        for path_info in self.sorting_centers[origin].package_path(origin, destination):
            next_event_time = event_time + path_info["travel_time"]
            result = {
                "sorting_center": origin,
                "event_time": int(event_time),
                "package_id": package_id,
                "scanner_id": current_scanner,
                "next_scanner_id": path_info["next"],
                "next_event_time": int(next_event_time),
            }
            if current_scanner == "intake":
                result["declared_value"] = random.randint(10, 100)
                result["destination"] = destination
                result["estimated_delivery_time"] = int(
                    self.get_travel_time(origin, destination) + event_time
                )
            elif current_scanner == "weighing":
                result["weight"] = random.randint(1, 40)
            elif not result["next_scanner_id"]:
                del result["next_event_time"]
                del result["next_scanner_id"]
            yield result
            # set time for next actual scan, must always be less than
            # expected scan time
            event_time = next_event_time - random.randint(0, SECONDS_PER_MINUTE)
            if event_time >= self.simulated_end_time:
                return

            current_scanner = path_info["next"]

        truck_travel_time = TRUCK_TRAVEL_TIMES[(origin, destination)]
        if not truck_travel_time:
            # package is delivered, no further routing is needed
            return


        # the truck to the next sorting center will be loaded at the top of the hour
        # there's no scan event for that, but that's when the truck will leave
        # for now, we do want all packages on this truck to arrive at the same
        # time, so we do need to calculate "top of the hour"

        # get current hour
        whole, _ = divmod(event_time, SECONDS_PER_HOUR)

        # top of 'next hour'
        receiving_event_time = SECONDS_PER_HOUR * (whole + 1)
        # add truck travel time
        receiving_event_time += truck_travel_time * SECONDS_PER_MINUTE

        # emit arriving at holding 
        result = {
            "sorting_center": origin,
            "event_time": int(event_time),
            "package_id": package_id,
            "scanner_id": current_scanner,
            "next_scanner_id": "receiving",
            "next_event_time": int(receiving_event_time),
        }

        yield result

        event_time = receiving_event_time
        current_scanner = "receiving"
        for path_info in self.sorting_centers[destination].package_path(
            origin, destination
        ):
            next_event_time = event_time + path_info["travel_time"]
            result = {
                "sorting_center": destination,
                "event_time": int(event_time),
                "package_id": package_id,
                "scanner_id": current_scanner,
                "next_scanner_id": path_info["next"],
                "next_event_time": int(next_event_time),
            }
            if not result["next_scanner_id"]:
                del result["next_event_time"]
                del result["next_scanner_id"]

            yield result
            # set time for next actual scan, must always be less than
            # expected scan time
            event_time = next_event_time - random.randint(0, SECONDS_PER_MINUTE)
            if event_time >= self.simulated_end_time:
                return

            current_scanner = path_info["next"]

    def get_travel_time(self, origin, destination):
        """return total estimated travel time"""
        origin_sorting_center = self.sorting_centers[origin]
        destination_sorting_center = self.sorting_centers[destination]
        if origin is destination:
            travel_time = (
                origin_sorting_center.INTAKE_TIME + origin_sorting_center.OUTPUT_TIME
            )
        else:
            travel_time = (
                origin_sorting_center.INTAKE_TIME
                + origin_sorting_center.HOLDING_TIME
                + destination_sorting_center.RECEIVING_TIME
                + destination_sorting_center.OUTPUT_TIME
                + TRUCK_TRAVEL_TIMES[(origin, destination)] * SECONDS_PER_MINUTE
            )
            # round up to the next hour to account for loading time on truck
            whole, _ = divmod(travel_time, SECONDS_PER_HOUR)

            travel_time = SECONDS_PER_HOUR * (whole + 1)

        # and for just to be safe, add another 30 minutes
        return travel_time + SECONDS_PER_MINUTE * 30

    def generate_lost_or_delayed_packages(
        self, package_count, lost_package_count, delayed_package_count
    ):
        """randomly select packages to delayed or lost"""
        result = {}  # key is package_id, value is delay info
        assert lost_package_count <= delayed_package_count
        assert delayed_package_count < package_count

        # lost packages are selected from delayed packages
        for idx, package_id in enumerate(
            random.sample(range(1, package_count), delayed_package_count)
        ):
            result[str(package_id)] = {
                "type": "lost" if idx < lost_package_count else "delayed",
                "delay": 2 * SECONDS_PER_HOUR,
                "event_index": random.choice(
                    (3, 3, 3, 3, 1, 2, 4)
                ),  # lose or delay most of them in routing
            }
        logging.debug("delay map %r", result)
        return result

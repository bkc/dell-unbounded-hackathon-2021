SORTING_CENTER_CODES = "ABCD"

SORTING_CENTER_TO_STREAM_NAME = {_: "sorting-center-input-%s" % _ for _ in "ABCD"}
TROUBLE_EVENT_STREAM_NAME = "trouble-events"

REDIS_PACKAGE_NEXT_EVENT_KEY_NAME = "next_package_event"
REDIS_CLOCK_SYNC_KEY_NAME = "clock_sync"
REDIS_LATE_PACKAGE_HASH_NAME = "late_packages"
REDIS_PACKAGE_NEXT_SCANNER_ID_KEY_NAME = "next_package_scanner"

ALL_REDIS_KEYS = (
    REDIS_PACKAGE_NEXT_EVENT_KEY_NAME,
    REDIS_CLOCK_SYNC_KEY_NAME,
    REDIS_LATE_PACKAGE_HASH_NAME,
)

MINIMUM_LATE_PACKAGE_SECONDS = (
    60  # package must be at least this many seconds late before we warn
)

PUBLIC_SCANNER_EVENTS = (
    "intake",
    "holding_A",
    "holding_B",
    "holding_C",
    "holding_D",
    "receiving",
    "output",
)  # these are public tracking events


PACKAGE_ATTRIBUTES_KVT_NAME = "package-attributes"
PACKAGE_EVENTS_KVT_NAME = "package-events"

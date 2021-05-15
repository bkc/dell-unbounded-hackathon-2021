SORTING_CENTER_CODES = "ABCD"

SORTING_CENTER_TO_STREAM_NAME = {_: "sorting-center-input-%s" % _ for _ in "ABCD"}

REDIS_PACKAGE_NEXT_EVENT_KEY_NAME = "next_package_event"

PUBLIC_SCANNER_EVENTS = (
    "intake",
    "holding A",
    "holding B",
    "holding C",
    "holding D",
    "receiving",
    "output",
)  # these are public tracking events


PACKAGE_ATTRIBUTES_KVT_NAME = "package-attributes"
PACKAGE_EVENTS_KVT_NAME = "package-events"

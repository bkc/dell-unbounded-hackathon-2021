import logging


def add_logging_argument(parser):
    valid_log_levels = ("info", "warn", "debug", "error", "fatal", "critical")

    parser.add_argument(
        "-l",
        "--console_log_level",
        default="info",
        choices=valid_log_levels,
        help="set logging level for console output: %s" % (",".join(valid_log_levels)),
    )


def setup_logging(options):
    """setup logging options at the provided log_level"""
    log_level = options.console_log_level.upper()

    # setup root logger to log everything
    root_logger = logging.getLogger("")
    root_logger.setLevel(log_level)

    # setup console handler with it's own logging level
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch_formatter = logging.Formatter(
        "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
    )
    ch.setFormatter(ch_formatter)
    root_logger.addHandler(ch)

    return log_level

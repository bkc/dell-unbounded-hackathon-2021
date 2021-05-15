import sys

if "java" in sys.platform:
    from redis.clients.jedis import Jedis
    is_java = True
else:
    from redis import Redis

    is_java = False


def add_redis_argparse_argument(parser):
    parser.add_argument("--rs", dest="redis_server", help="redis host[:port]")

    return parser


def get_redis_server_from_options(options):
    redis_info = None
    if options.redis_server:
        redis_host_port = options.redis_server.split(":")
        redis_info = {"host": redis_host_port[0], "port": 6379}
        if len(redis_host_port) > 1:
            redis_info["port"] = int(redis_host_port[1])

        if is_java:
            return Jedis(redis_info["host"], redis_info["port"])
        else:
            return Redis(**redis_info)

    return None

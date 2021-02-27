import os
import sys

import async_config_object
import async_server

CONFIG_OBJECT_LIST = [
    (async_config_object.ConfigObject(), os.path.join(os.getcwd(), "config", "async_lt.ini"))
]


def main():

    configs =
    for config_object, lt in CONFIG_OBJECT_LIST:

        optimal_concurrency = async_server.generate_latency_throughput(
            config_object, lt)


    return 0


if __name__ == "__main__":
    sys.exit(main())

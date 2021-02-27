import argparse
import sys

import async_config_object
import async_server
import generate_configs

CONFIG_OBJ_LIST = [
    async_config_object.ConfigObject(),
]


def main():
    parser = argparse.ArgumentParser(description="Determine a stable interval")
    parser.add_argument("--duration", type=int, default=20 * 60,
                        help="Duration (s) that to run test for")
    parser.add_argument("--csv_location", type=str,
                        default="scratch/stabilizer",
                        help="location of resulting csv file")
    parser.add_argument("--graph_location", type=str,
                        default="scratch/stabilizer",
                        help="location of resulting graph")
    args = parser.parse_args()

    # Generate configurations
    configs = []
    for config_object in CONFIG_OBJ_LIST:
        configs += config_object.generate_config_combinations()

    # Run each configuration
    for config in configs:
        # server
        async_server.build_server(config["server_node"],
                                  config["server_commit_branch"])
        async_server.run_server(config["server_node"],
                                config["server_concurrency"])

        # clients
        logfiles = async_server.run_clients(config["worknode_nodes"],
                                            config["server_node"],
                                            args.duration,
                                            config["client_concurrency"],
                                            config["batch"],
                                            config["read_percent"],
                                            args.csv_location)

        # graph
        dat_file = async_server.parse_raw_logfiles(logfiles, args.csv_location)
        async_server.graph(dat_file, args.graph_location)

    return 0


if __name__ == "__main__":
    sys.exit(main())

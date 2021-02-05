import argparse
import csv
import datetime
import functools
import os
import sys

import config_object
import constants
import csv_utils
import generate_configs
import parse_stable
import plot_utils
import run_single_data_point as rsdp
import trial_stabilize

###### configuring the main file #########

# configuration object generators matched to the latency throughput files


CONFIG_OBJ_LIST = [
    (trial_stabilize.ConfigObject(), os.path.join(constants.TEST_CONFIG_PATH, "lt_grpc_go.ini"))
]
unique_suffix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
DB_DIR = os.path.join(constants.ROOT, "thermopylae_tests/scratch",
                      "stabilizer_{0}".format(unique_suffix))

def generate_dir_name(config_fpath, db_dir):
    config_file = os.path.basename(config_fpath)
    config_name = config_file.split('.')[0]
    dir_name = os.path.join(db_dir, config_name)

    return dir_name


def main():
    parser = argparse.ArgumentParser(description="Determine a stable interval")
    parser.add_argument("--duration", type=int, default=20 * 60,
                        help="Duration (s) that to run test for")
    parser.add_argument("--csv_location", type=str,
                        default="scratch/stabilizer.csv",
                        help="location of resulting csv file")
    parser.add_argument("--graph_location", type=str,
                        default="scratch/stabilizer.png",
                        help="location of resulting graph")
    args = parser.parse_args()

    args = parser.parse_args()

    ### I've forgotten what this code does, but if it works, it works

    db_dir = DB_DIR
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    cfg_obj, lt_fpath = CONFIG_OBJ_LIST[0]
    files_to_process = os.path.join(db_dir, "configs_to_process.csv")
    cfg_fpath_list = cfg_obj.generate_all_config_files()
    data = [{constants.CONFIG_FPATH_KEY: cfg_fpath,
                     "lt_fpath": lt_fpath} for cfg_fpath in cfg_fpath_list]
    csv_utils.append_data_to_file(data, files_to_process)
    _, cfg_lt_tuples = csv_utils.read_in_data_as_tuples(files_to_process,
                                                        has_header=False)
    cfg_fpath, lt_fpath = cfg_lt_tuples[0]
    cfg = generate_configs.generate_configs_from_files_and_add_fields(cfg_fpath)
    log_dir = generate_dir_name(cfg[constants.CONFIG_FPATH_KEY], db_dir)
    config = cfg

    #### I've started remembering what the code does starting here

    rsdp.setup(config, log_dir)

    benchmark_logs = rsdp.run_kv_workload(client_nodes=config["workload_nodes"],
                         server_nodes=config["warm_nodes"],
                         concurrency=config["concurrency"],
                         keyspace=config["keyspace"],
                         warm_up_duration=config["warm_up_duration"],
                         duration=args.duration,
                         read_percent=config["read_percent"],
                         n_keys_per_statement=config["n_keys_per_statement"],
                         skew=config["skews"],
                         log_dir=log_dir,
                         mode=rsdp.RunMode.TRIAL_RUN_ONLY)

    print(benchmark_logs)

    # parse out benchmark log files and graph them
    row_dicts = map(parse_stable.parse_file, benchmark_logs)
    datasets = map(functools.partial(parse_stable.create_rowdicts_from_dataset,
                                     x_axis="elapsed", y_axis="ops_cumul"),
                   row_dicts)
    final_dataset = functools.reduce(parse_stable.add_datasets, datasets)
    final_rowdicts = parse_stable.create_rowdicts_from_dataset(final_dataset,
                                                               x_axis="elapsed",
                                                               y_axis="ops_cumul")
    with open(args.csv_location, "w") as f:
        writer = csv.DictWriter(f, fieldnames=final_rowdicts[0].keys())
        writer.writeheader()
        writer.writerows(rowdicts=final_rowdicts)

    plot_utils.gnuplot("src/gnuplot/determine_stable_warmup.gp",
                       args.csv_location, args.graph_location, "elapsed (s)",
                       "throughput (tps)", "elapsed", "ops_cumul")

    return 0


if __name__ == "__main__":
    sys.exit(main())

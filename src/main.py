import datetime
import os
import sqlite3

import config_io
import config_object
import constants
import csv_utils
import generate_configs
import latency_throughput
import run_single_data_point
import sqlite_helper_object
import system_utils

######## configuring the main file ###########

# configuration objec generators matched to the latency throughput files
CONFIG_OBJ_LIST = [
  (config_object.ConfigObject(), os.path.join(constants.TEST_CONFIG_PATH, "override.ini")),
]

# location of the entire database run
unique_suffix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
DB_DIR = os.path.join(constants.SCRATCH_DIR, "db_{0}".format(unique_suffix))

######## end of configs #############


def generate_dir_name(config_fpath):
  config_file = os.path.basename(config_fpath)
  config_name = config_file.split('.')[0]
  dir_name = os.path.join(DB_DIR, config_name)

  return dir_name

# def table_rows():
#   fields = ["elapsed-r",
#             "errors-r",
#             "ops(total)-r",
#             "ops/sec(cum)-r",
#             "avg(ms)-r",
#             "p50(ms)-r",
#             "p95(ms)-r",
#             "p99(ms)-r",
#             "pMax(ms)-r",
#             "total-r",
#             "elapsed-w",
#             "errors-w",
#             "ops(total)-w",
#             "ops/sec(cum)-w",
#             "avg(ms)-w",
#             "p50(ms)-w",
#             "p95(ms)-w",
#             "p99(ms)-w",
#             "pMax(ms)-w",
#             "total-w",
#             "elapsed",
#             "errors",
#             "ops(total)",
#             "ops/sec(cum)",
#             "avg(ms)",
#             "p50(ms)",
#             "p95(ms)",
#             "p99(ms)",
#             "pMax(ms)",
#   ]
#
#   return "({0})".format(", ".join(fields))
#
#
#


def main():

  # create the database and table
  if not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR)
  db = sqlite_helper_object.SQLiteHelperObject(os.path.join(DB_DIR, "trials.db"))
  db.connect()

  # file of failed configs
  failed_configs_csv = os.path.join(DB_DIR, "failed_configs.csv")

  for cfg_obj, lt_fpath in CONFIG_OBJ_LIST:

    # generate config objects
    cfg_fpath_list = cfg_obj.generate_all_config_files()
    cfgs = generate_configs.generate_configs_from_files_and_add_fields(cfg_fpath_list)

    # generate lt_config objects that match those config objects
    lt_cfg = config_io.read_config_from_file(lt_fpath)

    for cfg in cfgs:
      try:
        # make directory in which trial will be run
        logs_dir = generate_dir_name(cfg["config_fpath"])
        if not os.path.exists(logs_dir):
          os.makedirs(logs_dir)

        # copy over config into directory
        system_utils.call("cp {0} {1}".format(cfg["config_fpath"], logs_dir))

        # generate latency throughput trials
        lt_fpath_csv = latency_throughput.run(cfg, lt_cfg, logs_dir)

        # run trial
        cfg["concurrency"] = latency_throughput.find_optimal_concurrency(lt_fpath_csv)
        results_fpath_csv = run_single_data_point.run(cfg, logs_dir)

        # insert into sqlite db
        # TODO get the actual commit hash, not the branch
        db.insert_csv_data_into_sqlite_table("trials_table", results_fpath_csv,
                                             {"logs_dir": logs_dir,
                                              "cockroach_commit": cfg["cockroach_commit"],
                                              "server_nodes": cfg["num_warm_nodes"],
                                              "disabled_cores": cfg["disable_cores"],
                                              "keyspace": cfg["keyspace"],
                                              "read_percent": cfg["read_percent"],
                                              "n_keys_per_statement": cfg["n_keys_per_statement"],
                                              "skews": cfg["skews"]})

      except BaseException as e:
        print("Config {0} failed to run, continue with other configs. e:[{1}]"
              .format(cfg["config_fpath"], e))
        csv_utils.write_out_data({"config_fpath": cfg["config_fpath"],
                                  "lt_fpath": lt_fpath},
                                 failed_configs_csv)

  db.close()


if __name__ == "__main__":
  main()

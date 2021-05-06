import os
import shlex
import subprocess

import constants
import csv_utils
import gather
import system_utils
from run_single_data_point import EXE
import run_single_data_point as rsdp


def set_cluster_settings_on_single_node(node):
    rsdp.set_cluster_settings_on_single_node(node)
    # cmd=('echo "'
    #      'SET CLUSTER SETTING rocksdb.ingest_backpressure.l0_file_count_threshold = 100; '
    #      'SET CLUSTER SETTING rocksdb.ingest_backpressure.pending_compaction_threshold = \'5 GiB\'; '
    #      'SET CLUSTER SETTING schemachanger.backfiller.max_buffer_size = \'5 GiB\'; '
    #      'SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = \'128 MiB\'; '
    #      'SET CLUSTER SETTING rocksdb.min_wal_sync_interval = \'500us\';')


def build_cockroachdb_workload_commit_on_single_node(node, commit_hash):
    cmd = ("ssh {0} 'export GOPATH={3}/go "
           "&& set -x && cd {1} && git fetch origin {2} && git stash "
           "&& git checkout {2} && git pull origin {2} "
           "&& git submodule update --init "
           "&& export PATH=$PATH:/usr/local/go/bin "
           "&& echo $PATH && make bin/workload && set +x'")\
        .format(node["ip"], constants.COCKROACHDB_DIR, commit_hash, constants.ROOT)

    return subprocess.Popen(shlex.split(cmd))


def build_cockroachdb_workload_commit(nodes, commit_hash):
    processes = [build_cockroachdb_workload_commit_on_single_node(node, commit_hash) for node in nodes]
    for process in processes:
        process.wait()


def run_tpcc_workload(client_nodes, server_nodes, warehouses,
                      warm_up_duration, duration, log_dir,
                      concurrency):

    server_urls = ["postgresql://root@{0}:26257?sslmode=disable".format(n["ip"])
                   for n in server_nodes]


    # populate tpcc data
    import_tpcc_data_cmd = ("{0}/bin/workload fixtures import tpcc "
                            "--warehouses {1} "
                            "{2} ")\
        .format(constants.COCKROACHDB_DIR, warehouses, server_urls[0])
    driver_node = client_nodes[0]
    system_utils.call_remote(driver_node["ip"], import_tpcc_data_cmd)

    # making the logs directory, if it doesn't already exist
    log_fpath = os.path.join(log_dir, "logs")
    if not os.path.exists(log_fpath):
        os.makedirs(log_fpath)

    # run workload
    args = ["--warehouses {}".format(warehouses),
            "--ramp {}s".format(warm_up_duration),
            "--duration {}s".format(duration),
            "--concurrency {}".format(concurrency)]

    trial_processes = []
    bench_log_files = []
    for i in range(len(client_nodes)):
        node = client_nodes[i]

        # logging output for each node
        individual_log_fpath = os.path.join(
            log_fpath, "bench_{}.txt".format(node["ip"]))
        bench_log_files.append(individual_log_fpath)

        # run command
        workload_cmd = "{0}/bin/workload run tpcc {1} {2}"\
            .format(constants.COCKROACHDB_DIR, " ".join(args),
                    server_urls[i % len(server_nodes)])
        individual_node_cmd = "sudo ssh {0} '{1}'"\
            .format(node["ip"], workload_cmd)
        print(individual_node_cmd)
        with open(individual_log_fpath, "w") as f:
            trial_processes.append(subprocess.Popen(
                shlex.split(individual_node_cmd), stdout=f))

    for tp in trial_processes:
        tp.wait()

    return bench_log_files


def run(config, log_dir):
    server_nodes = config["warm_nodes"]
    client_nodes = config["workload_nodes"]
    commit_hash = config["cockroach_commit"]
    hot_node = config["hot_node"] if "hot_node" in config else None

    # clear any remaining experiments
    rsdp.cleanup_previous_experiments(server_nodes, client_nodes, hot_node)

    # disable cores, if need be
    cores_to_disable = config["disable_cores"]
    if cores_to_disable > 0:
        rsdp.disable_cores(server_nodes, cores_to_disable)
        if hot_node:
            rsdp.disable_cores([hot_node], cores_to_disable)

    # start hot node
    min_key = 0
    if hot_node:
        rsdp.setup_hotnode(hot_node, config["hot_node_commit_branch"],
                           config["hot_node_concurrency"])
        min_key = config["hot_node_threshold"]

    # build and start crdb cluster
    rsdp.build_cockroachdb_commit(server_nodes + client_nodes, commit_hash)
    rsdp.start_cluster(server_nodes)
    set_cluster_settings_on_single_node(server_nodes[0])

    # build and start client nodes
    build_cockroachdb_workload_commit(client_nodes, commit_hash)
    warehouses = config["warehouses"]
    warm_up_duration = config["warm_up_duration"]
    duration = config["duration"]
    concurrency = config["concurrency"]
    bench_log_files = run_tpcc_workload(client_nodes,
                                        server_nodes,
                                        warehouses,
                                        warm_up_duration,
                                        duration,
                                        log_dir,
                                        concurrency)

    # # create csv file of gathered data
    # data = {"concurrency": config["concurrency"]}
    # more_data, has_data = gather.gather_data_from_raw_tpcc_logs(bench_log_files)
    # if not has_data:
    #     raise RuntimeError(
    #         "Config {0} has failed to produce any results".format(
    #             config[constants.CONFIG_FPATH_KEY])
    #     )
    # data.update(more_data)
    #
    # # write out csv file
    # results_fpath = os.path.join(log_dir, "results.csv")
    # _ = csv_utils.write_out_data([data], results_fpath)

    # re-enable cores
    cores_to_enable = cores_to_disable
    if cores_to_enable > 0:
        rsdp.enable_cores(server_nodes, cores_to_enable)
        if hot_node:
            rsdp.enable_cores([hot_node], cores_to_enable)

    # return results_fpath
    return bench_log_files


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("ini_file")
    parser.add_argument("concurrency", type=int)
    parser.add_argument("--log_dir", type=str, default=constants.SCRATCH_DIR)
    args = parser.parse_args()

    import config_io
    config = config_io.read_config_from_file(args.ini_file)
    config["concurrency"] = args.concurrency
    import datetime
    unique_suffix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    log_dir = os.path.join(args.log_dir, "run_single_trial_{0}".format(unique_suffix))
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    run(config, log_dir)
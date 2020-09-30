import os
import shlex
import subprocess
import time

import constants
import csv_utils
import gather
import run_single_data_point

SERVER_EXE = "/usr/local/grpc/examples/cpp/helloworld/cmake/build/greeter_async_server"
CLIENT_DIR = os.path.join(constants.GRPC_GO_DIR, "benchmark")
CLIENT_EXE = "./run_bench.sh"

# def kill_client_node(node):
#     ip = node["ip"]
#
#     cmd = ("PID=$(! pgrep 'port=50051') "
#            "|| (sudo pkill -9 'port=50051'; while ps -p $PID;do sleep 1;done;)")
#
import system_utils


def kill_server_node(node):
    ip = node["ip"]

    cmd = ("PID=$(! pgrep greeter_async_server) "
           "|| (sudo pkill -9 greeter_async_server; while ps -p $PID;do sleep 1;done;)")

    system_utils.call_remote(ip, cmd)


def cleanup_previous_experiments(server_node, client_nodes):
    # # kill all client nodes TODO implement killing of client nodes
    # client_processes = [kill_client_node(node) for node in client_nodes]
    # for cp in client_processes:
    #     cp.wait()

    # kill server node
    kill_server_node(server_node)

    run_single_data_point.enable_cores([server_node], 15)


def start_server(server_node):
    cmd = "sudo ssh {0} '{1}'".format(server_node["ip"], SERVER_EXE)

    subprocess.Popen(shlex.split(cmd))
    time.sleep(2)


def run_client_workload(client_nodes, server_node, rpcs_per_conn, connections_per_node, warm_up_duration, duration,
                        req, resp, rpc_type, log_dir):
    args = ["-w {}".format(warm_up_duration),
            "-d {}".format(duration),
            "-r {}".format(rpcs_per_conn),
            "-c {}".format(connections_per_node),
            "-req {}".format(req),
            "-resp {}".format(resp),
            "-rpc_type {}".format(rpc_type),
            "-addr {}".format(server_node["ip"]),
            ]

    cmd = "cd {0}; {1} {2}".format(CLIENT_DIR, CLIENT_EXE, " ".join(args))

    log_fpath = os.path.join(log_dir, "logs")
    if not os.path.exists(log_fpath):
        os.makedirs(log_fpath)

    bench_log_files = []
    processes = []
    for node in client_nodes:
        # logging output for each node
        individual_node_fpath = os.path.join(log_fpath, "bench_{}.txt".format(node["ip"]))
        bench_log_files.append(individual_node_fpath)

        # run command
        individual_node_cmd = "sudo ssh {0} '{1}'".format(node["ip"], cmd)
        print(individual_node_cmd)
        with open(individual_node_fpath, "w") as f:
            processes.append(subprocess.Popen(shlex.split(individual_node_cmd), stdout=f))

    for p in processes:
        p.wait()

    return bench_log_files


def git_fetch_commit_on_single_node(node, commit_hash):
    cmd = ("ssh {0} 'export GOPATH=/usr/local/temp/go "
           "&& set -x && cd {1} && git fetch origin {2} && git checkout {2} && git pull origin {2} "
           "&& export PATH=$PATH:/usr/local/go/bin && echo $PATH && set +x'") \
        .format(node["ip"], constants.GRPC_GO_DIR, commit_hash)
    print(cmd)

    return subprocess.Popen(shlex.split(cmd))


def git_fetch_commit(client_nodes, commit_hash):
    processes = [git_fetch_commit_on_single_node(node, commit_hash) for node in client_nodes]
    for process in processes:
        process.wait()


def run(config, log_dir):
    """

    Args:
        config:
        log_dir:

    Returns:

    """

    server_node = config["addr"]
    client_nodes = config["client_nodes"]

    cleanup_previous_experiments(server_node, client_nodes)

    # disable cores, if need be
    cores_to_disable = config["disable_cores"]
    if cores_to_disable > 0:
        run_single_data_point.disable_cores([server_node], cores_to_disable)

    # start server
    start_server(server_node)

    # start clients
    commit_hash = config["grpc_go_commit"]
    git_fetch_commit(client_nodes, commit_hash)
    rpcs_per_conn = config["rpcs"]
    connections_per_node = config["concurrency"]
    warm_up_duration = config["warm_up_duration"]
    duration = config["duration"]
    req = config["req"]
    resp = config["resp"]
    rpc_type = config["rpc_type"]

    bench_log_files = run_client_workload(client_nodes, server_node, rpcs_per_conn, connections_per_node,
                                          warm_up_duration, duration, req, resp, rpc_type, log_dir)

    # create csv file of gathered data
    data = {"concurrency": config["concurrency"]}
    more_data, has_data = gather.gather_data_from_raw_grpc_benchmark_logs(bench_log_files)
    if not has_data:
        raise RuntimeError("Config {0} has failed to produce any results"
                           .format(config[constants.CONFIG_FPATH_KEY]))
    data.update(more_data)

    # write out csv file
    results_fpath = os.path.join(log_dir, "results.csv")
    _ = csv_utils.write_out_data([data], results_fpath)

    # re-enable cores
    cores_to_enable = cores_to_disable
    if cores_to_enable > 0:
        run_single_data_point.enable_cores([server_node], cores_to_enable)

    return results_fpath


def main():
    import config_object_grpc_benchmark
    config_obj = config_object_grpc_benchmark.ConfigObject()
    single_config = config_obj.generate_config_combinations()[0]

    log_dir = "../test"
    run(single_config, log_dir)


if __name__ == "__main__":
    main()

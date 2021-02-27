import system_utils


def build_server(server_node, commit_branch):
    server_url = server_node["ip"]

    cmd = "cd /root/smdbrpc; " \
          "git fetch origin {0}; " \
          "git checkout origin {0}; " \
          "git pull origin {0}; ".format(commit_branch)
    print(system_utils.call_remote(server_url, cmd))

    cmd = "cd /root/smdbrpc/cpp/; " \
          "rm -rf cmake/*; " \
          "mkdir -p cmake/build; " \
          "pushd cmake/build; " \
          "cmake -DCMAKE_PREFIX_PATH=/root/.local ../..; " \
          "make -j"
    print(system_utils.call(server_url, cmd))


def run_server(server_node, concurrency):
    server_url = server_node["ip"]

    cmd = "/root/smdbrpc/cpp/cmake/build/hotshard_gateway_async_server {0}" \
        .format(concurrency)

    print(system_utils.call_remote(server_url, cmd))


def run_clients(client_nodes, server_node, duration, concurrency, batch, read_percent,
                location):
    server_url = server_node["ip"]

    client_processes = []
    i = 0
    logfiles = []
    for client in client_nodes:
        log = os.path.join(location, "raw_data{0}.txt".format(i))
        client_url = client["ip"]
        cmd = "/root/smdbrpc/go/hotshard_gateway_client/generate_workload_client " \
              "--concurrency {0} " \
              "--batch {1} " \
              "--read_percent {2}" \
              "--host {3} " \
              "--instantaneousStats " \
              "--duration {4}s".format(concurrency,
                                       batch,
                                       read_percent,
                                       server_url,
                                       duration)
        ssh_wrapped_cmd = "sudo ssh {0} '{1}'".format(client_url, cmd)
        with open(log, "w") as f:
            client_processes.append(subprocess.Popen(
                shlex.split(ssh_wrapped_cmd), stdout=f))

        i = i + 1
        logfiles.append(log)

    for cp in client_processes:
        cp.wait()

    return logfiles


def parse_raw_logfiles(input_logfiles, output_csvfile):
    return "tee hee"


def graph(datafile, graph_location):
    return "more tee hee"

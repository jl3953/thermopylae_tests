import system_utils

import os
import shlex
import subprocess


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
          "export PATH=$PATH:/root/.local/bin; " \
          "cmake -DCMAKE_INSTALL_PREFIX=/root/.local ../..; " \
          "make -j"
    print(system_utils.call_remote(server_url, cmd))


def build_client(client_node, commit_branch):
    client_url = client_node["ip"]

    cmd = "cd /root/smdbrpc; " \
          "git fetch origin {0}; " \
          "git checkout origin {0}; " \
          "git pull origin {0}; ".format(commit_branch)
    print(system_utils.call_remote(client_url, cmd))


def run_server(server_node, concurrency):
    server_url = server_node["ip"]

    cmd = "/root/smdbrpc/cpp/cmake/build/hotshard_gateway_async_server {0}" \
        .format(concurrency)
    ssh_wrapped_cmd = "sudo ssh {0} '{1}'".format(server_url, cmd)

    process = subprocess.Popen(shlex.split(ssh_wrapped_cmd))
    return process


def run_clients(client_nodes, server_node, duration, concurrency, batch, read_percent,
                location):
    server_url = server_node["ip"]

    client_processes = []
    i = 0
    logfiles = []
    for client in client_nodes:
        log = os.path.join(location, "raw_data{0}.txt".format(i))
        client_url = client["ip"]
        cmd = "cd /root/smdbrpc/go; " \
              "/usr/local/go/bin/go run hotshard_gateway_client/generate_workload_client.go " \
              "--concurrency {0} " \
              "--batch {1} " \
              "--read_percent {2} " \
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
                shlex.split(ssh_wrapped_cmd), stdout=f, stderr=f))

        i = i + 1
        logfiles.append(log)

    for cp in client_processes:
        cp.wait()

    return logfiles


def kill(node):
    ip = node["ip"]

    cmd = ("PID=$(! pgrep hotshard) "
           "|| (sudo pkill -9 hotshard; while ps -p $PID;do sleep 1;done;)")

    system_utils.call_remote(ip, cmd)


def parse_raw_logfiles(input_logfiles, output_csvfile):
    return "tee hee"


def graph(datafile, graph_location):
    return "more tee hee"

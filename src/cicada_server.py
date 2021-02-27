import shlex
import subprocess
import time

import async_server
import system_utils


def build_server(server_node, commit_branch):
    server_url = server_node["ip"]

    cmd = "cd /root/cicada-engine; " \
          "git fetch origin {0}; " \
          "git checkout origin {0}; " \
          "git pull origin {0}; ".format(commit_branch)
    print(system_utils.call_remote(server_url, cmd))

    cmd = "cd /root/cicada-engine/build; " \
          "rm -rf *; " \
          "export PATH=$PATH:/root/.local/bin; " \
          "cmake -DLTO=ON ..; " \
          "make -j; " \
          "../script/setup.sh 16384 16384; " \
          "ln -s src/mica/test/*.json ."
    print(system_utils.call_remote(server_url, cmd))


def build_client(client_node, commit_branch):
    async_server.build_client(client_node, commit_branch)


def run_server(server_node, concurrency):
    server_url = server_node["ip"]

    cmd = "/root/cicada-engine/build/hotshard_gateway_server {0}" \
        .format(concurrency)
    ssh_wrapped_cmd = "sudo ssh {0} '{1}'".format(server_url, cmd)

    process = subprocess.Popen(shlex.split(ssh_wrapped_cmd))
    time.sleep(30)
    return process


def run_clients(client_nodes, server_node, duration, concurrency, batch, read_percent,
                location):
    async_server.run_clients(client_nodes,
                             server_node,
                             duration,
                             concurrency,
                             batch,
                             read_percent,
                             location)


def kill(node):
    async_server.kill(node)


def parse_raw_logfiles(input_logfiles, output_csvfile):
    return async_server.parse_raw_logfiles(input_logfiles, output_csvfile)


def graph(datafile, graph_location):
    return async_server.graph(datafile, graph_location)

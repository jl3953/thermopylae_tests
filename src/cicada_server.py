import os
import shlex
import subprocess
import time

import grpc
import smdbrpc_pb2
import smdbrpc_pb2_grpc

import async_server
import system_utils


def build_server(server_node, commit_branch):
    server_url = server_node["ip"]

    cmd = "cd /root/cicada-engine; " \
          "git fetch origin {0}; " \
          "git stash; " \
          "git checkout origin {0}; " \
          "git pull origin {0}; ".format(commit_branch)
    print(system_utils.call_remote(server_url, cmd))

    cmd = "cd /root/cicada-engine/build; " \
          "rm -rf *; " \
          "export PATH=$PATH:/root/.local/bin; " \
          "cmake -DLTO=ON -DDEBUG=OFF ..; " \
          "make -j; " \
          "/root/cicada-engine/script/setup.sh 16384 16384; " \
          "cp /root/cicada-engine/src/mica/test/test_tx.json /root/cicada-engine/build/"
    print(system_utils.call_remote(server_url, cmd))


def build_client(client_node, commit_branch):
    async_server.build_client(client_node, commit_branch)


def run_server(server_node, concurrency, log_dir, threshold):
    server_url = server_node["ip"]

    cmd = "/root/cicada-engine/build/hotshard_gateway_server {0}" \
        .format(concurrency)
    ssh_wrapped_cmd = "sudo ssh {0} '{1}'".format(server_url, cmd)

    log_fpath = os.path.join(log_dir, "logs")
    if not os.path.exists(log_fpath):
        os.makedirs(log_fpath)

    cicada_log = os.path.join(log_fpath, "cicada_log.txt")
    with open(cicada_log, "w") as f:
        process = subprocess.Popen(shlex.split(ssh_wrapped_cmd), stdout=f)
    time.sleep(10)

    # pre-populate the data
    print(server_url)
    with grpc.insecure_channel("node-11:50051") as channel:
        stub = smdbrpc_pb2_grpc.HotshardGatewayStub(channel)
        write_keyset = []
        for i in range(threshold):
            write_keyset.append(smdbrpc_pb2.KVPair(
                key=i, value=i))

        try_again = True
        while try_again:
            response = stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
                hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                    walltime=1,
                    logicaltime=0,
                ),
                write_keyset=write_keyset
            ))
            if response.is_committed:
                try_again = False
            else:
                response = stub.ContactHotshard(smdbrpc_pb2.HotshardRequest(
                    hlctimestamp=smdbrpc_pb2.HLCTimestamp(
                        walltime=2,
                        logicaltime=0,
                    ),
                    read_keyset=[threshold-1],
                ))
                print(response)

    return process


def run_clients(client_nodes, server_node, duration, concurrency, batch, read_percent,
                location):
    return async_server.run_clients(client_nodes,
                                    server_node,
                                    duration,
                                    concurrency,
                                    batch,
                                    read_percent,
                                    location)


def kill(node):
    async_server.kill(node)


def aggregate_raw_logs(logfiles):
    return async_server.aggregate_raw_logs(logfiles)


def parse_raw_logfiles(input_logfiles, output_csvfile):
    return async_server.parse_raw_logfiles(input_logfiles, output_csvfile)


def graph(datafile, graph_location):
    return async_server.graph(datafile, graph_location)

import itertools

import config_io
import config_object
import node


class ConfigObject:

    def __init__(self):

        # default
        self.logs_dir = ["grpc-go-benchmark"]

        # cluster
        self.grpc_go_commit = ["benchmark_hotshard"]
        self.addr = [vars(node.Node(8))]
        self.client_nodes = [[vars(node.Node(i)) for i in range(1, 5)]]
        self.disable_cores = [0]

        # benchmark
        self.warm_up_duration = [10] # warm-up duration in seconds
        self.duration = [2] # benchmark duration in seconds
        self.rpcs = [1] # number of RPCs
        self.req = [1] # request size in bytes
        self.resp = [1] # response size in bytes
        self.rpc_type = ["unary"] # unary | streaming
        # self.connections = [1] # concurrencies

    def generate_config_combinations(self):

        temp_dict = vars(self)

        all_field_values = list(temp_dict.values())
        values_combinations = list(itertools.product(*all_field_values))

        combinations = []
        for combo in values_combinations:
            config_dict = dict(zip(temp_dict.keys(), combo))
            combinations.append(config_dict)

        return combinations

    def generate_all_config_files(self):
        """Generates all configuration files with different combinations of parameters.
    :return:
    """
        ini_fpaths = []
        config_combos = self.generate_config_combinations()
        for config_dict in config_combos:
            ini_fpath = config_object.ConfigObject.generate_ini_filename(suffix=config_dict["logs_dir"])
            ini_fpaths.append(config_io.write_config_to_file(config_dict, ini_fpath))

        return ini_fpaths
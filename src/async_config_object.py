import itertools
import config_object as co
import node


class ConfigObject:

    def __init__(self):

        # default
        self.trials = [i for i in range(1)]

        # server
        self.server_concurrency = [1]
        self.server_commit_branch = ["async"]
        self.server_node_ip_enum = [2]  # 196.168.1.???
        # self.server_node = [some Node object]

        # client
        self.num_workload_nodes = [2]
        self.client_concurrency = [10]
        self.driver_node_ip_enum = [i + 1 for i in self.server_node_ip_enum]  # 192.168.1.???
        self.duration = [30]  # duration of trial in seconds
        # self.workload_nodes [some Node objects]

        # workload
        self.batch = [1]  # keys per rpc
        self.read_percent = [95]

    def generate_config_combinations(self):
        """Generates the trial configuration parameters for a single run,
        lists all in a list of dicts.

        :return: a list of dictionaries of combinations
        """

        temp_dict = vars(self)
        all_field_values = list(temp_dict.values())
        values_combinations = list(itertools.product(*all_field_values))

        combinations = []
        for combo in values_combinations:
            config_dict = dict(zip(temp_dict.keys(), combo))
            combinations.append(config_dict)

        for config_dict in combinations:
            driver_node_ip_enum = config_dict["driver_node_ip_enum"]
            num_workload_nodes = config_dict["num_workload_nodes"]
            workload_nodes = co.ConfigObject.enumerate_workload_nodes(
                driver_node_ip_enum, num_workload_nodes)
            config_dict["workload_nodes"] = [vars(n) for n in workload_nodes]

            server_node_ip_enum = config_dict["server_node_ip_enum"]
            server_node = ConfigObject.create_server_node(server_node_ip_enum)
            config_dict["server_node"] = vars(server_node)

        return combinations

    @staticmethod
    def create_server_node(server_node_ip_enum):
        return node.Node(server_node_ip_enum)


def main():
    config_object = ConfigObject()
    print(config_object.generate_config_combinations())


if __name__ == "__main__":
    main()

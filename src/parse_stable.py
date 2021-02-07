import collections
import enum

import csv_utils


ELAPSED = 0
ERRORS = 1
OPS_INST = 2
OPS_CUMUL = 3
P50 = 4
P95 = 5
P99 = 6
PMAX = 7
OP_TYPE = 8


def parse_single_line(line):
    values = line.split()

    elapsed = float(values[ELAPSED].strip("s"))
    errors = float(values[ERRORS].strip())
    ops_inst = float(values[OPS_INST].strip())
    ops_cumul = float(values[OPS_CUMUL].strip())
    p50 = float(values[P50].strip())
    p95 = float(values[P95].strip())
    p99 = float(values[P99].strip())
    pMax = float(values[PMAX].strip())
    op_type = values[OP_TYPE].strip()

    return elapsed, errors, ops_inst, ops_cumul, p50, p95, p99, pMax, op_type


def parse_header(_):
    return "elapsed", "errors", "ops_inst", "ops_cumul", "p50", "p95", "pMax", "op_type"


def parse_file(filename):
    """ Parses a CockroachDB KV benchmark result file. Every line is a dictionary.

    Args:
        filename (str): full path of result file

    Returns:
        A list of dictionaries, each one representing a line in the result file.
    """
    is_first_line = True
    keys = []
    row_dicts = []
    with open(filename, "r") as f:
        for line in f:
            print(line)
            if is_first_line:
                is_first_line = False
                keys = parse_header(line)
            else:
                try:
                    values = parse_single_line(line)
                    data_point = dict(zip(keys, values))
                    row_dicts.append(data_point)
                except BaseException as e:
                    print("We're done parsing!", e)
                    break
                    # We've finished parsing the file

    return row_dicts


def create_dataset_from_rowdicts(row_dicts, x_axis="elapsed",
                                 y_axis="ops_cumul"):
    """Transforms a list of dictionaries, all with the same keys, into a
    single dictionary with only the two attributes given.

    Args:
        row_dicts (list[dict]): list of dictionaries, all with the same keys.
        x_axis (str): attribute that will serve as result dictionary's key
        y_axis (str): attribute that will serve as result dictionary'es value.

    Returns:
        A single dictionary with the keys being all the x_axis values from
        the list of dictionaries and all the values being the y_axis values.
    """

    dataset = collections.defaultdict(int)
    for data_point in row_dicts:
        time_point = data_point[x_axis]
        ops_at_time_point = data_point[y_axis]
        dataset[time_point] += ops_at_time_point

    return dataset


def add_dictionaries(dict1, dict2):
    for key, val in dict2.items():
        dict1[key] += val

    return dict1


def create_rowdicts_from_dataset(dataset, x_axis="elapsed", y_axis="ops_cumul"):
    row_dicts = []
    for key, val in dataset.items():
        row_dicts.append({
            x_axis: key,
            y_axis: val,
        })
    return row_dicts

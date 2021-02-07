import collections
import enum

import csv_utils


class Indices(enum.Enum):
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

    elapsed = float(values[Indices.ELAPSED].trim("s"))
    errors = float(values[Indices.ERRORS].trim())
    ops_inst = float(values[Indices.OPS_INST].trim())
    ops_cumul = float(values[Indices.OPS_CUMUL].trim())
    p50 = float(values[Indices.P50].trim())
    p95 = float(values[Indices.P95].trim())
    p99 = float(values[Indices.P99].trim())
    pMax = float(values[Indices.PMAX].trim())
    op_type = values[Indices.OP_TYPE].trim()

    return elapsed, errors, ops_inst, ops_cumul, p50, p95, p99, pMax, op_type


def parse_header(_):
    return "elapsed", "errors", "ops_inst", "ops_cumul", "p50", "p95", "pMax", "op_type"


def parse_file(filename):
    is_first_line = True
    keys = []
    row_dicts = []
    with open(filename, "r") as f:
        for line in f:
            if is_first_line:
                is_first_line = False
                keys = parse_header(line)
            else:
                try:
                    values = parse_single_line(line)
                    data_point = zip(keys, values)
                    data_points.append(data_point)
                except BaseException:
                    print("We're done parsing!")
                    # We've finished parsing the file

    return row_dicts


def create_dataset_from_rowdicts(row_dicts, x_axis="elapsed", y_axis="ops_cumul"):
    dataset = collections.defaultdict(int)
    for data_point in row_dicts:
        time_point = data_point[x_axis]
        ops_at_time_point = data_point[y_axis]
        dataset[time_point] += ops_at_time_point

    return dataset


def add_datasets(dataset1, dataset2):
    dataset = collections.defaultdict(int)
    for key, val in dataset1.iteritems():
        dataset[key] += val

    for key, val in dataset2.iteritems():
        dataset[key] += val

    return dataset


def create_rowdicts_from_dataset(dataset, x_axis="elapsed", y_axis="ops_cumul"):
    row_dicts = map(lambda key: dict(zip([x_axis, y_axis], [key, dataset[key]])), dataset)
    return row_dicts

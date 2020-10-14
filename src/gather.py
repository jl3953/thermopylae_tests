import collections
import re


def extract_data(last_eight_lines):
    def parse(header_line, data_line):
        if "elapsed" not in header_line:
            return {}

        suffix = ""
        fields = data_line.strip().split()
        if "read" in fields[-1]:
            suffix = "-r"
        elif "write" in fields[-1]:
            suffix = "-w"

        header = [w + suffix for w in re.split('_+', header_line.strip().strip('_'))]
        data = dict(zip(header, fields))
        return data

    read_data = {}
    try:
        read_data = parse(last_eight_lines[0], last_eight_lines[1])
    except BaseException:
        print("write only")

    try:
        write_data = parse(last_eight_lines[3], last_eight_lines[4])
        read_data.update(write_data)
    except BaseException:
        print("read only")

    data = parse(last_eight_lines[6], last_eight_lines[7])

    read_data.update(data)

    return read_data


def is_output_okay(tail):
    try:
        if not ("elapsed" in tail[3] and "elapsed" in tail[6]):
            return False

        return True
    except BaseException:
        return False


def aggregate(acc):
    final_datum = collections.defaultdict(float)
    for datum in acc:
        for k, v in datum.items():
            try:
                final_datum[k] += float(v)
            except BaseException:
                print("could not add to csv file key:[{0}], value:[{1}]".format(k, v))
                continue

    for k in final_datum:
        if "ops" not in k:
            final_datum[k] /= len(acc)

    return final_datum


def gather_data_from_raw_kv_logs(log_fpaths):
    acc = []

    for path in log_fpaths:

        with open(path, "r") as f:
            # read the last eight lines of f
            print(path)
            tail = f.readlines()[-8:]
            if not is_output_okay(tail):
                print("{0} missing some data lines".format(path))
                return None, False

            try:
                datum = extract_data(tail)
                acc.append(datum)
            except BaseException:
                print("failed to extract data: {0}".format(path))
                return None, False

    final_datum = aggregate(acc)
    return final_datum, True


def helper(latency_with_unit):
    try:
        return float(latency_with_unit.strip("µs")) / 1000.0
    except BaseException:
        print("oops, not microseconds, let's try milliseconds")

    try:
        return float(latency_with_unit.strip("ms"))
    except BaseException:
        print("oops, not milliseconds, let's try seconds")

    return float(latency_with_unit.strip("s")) * 1000.0


def extract_data_grpc_benchmark(head):
    tp_line = head[-2].strip()
    latency_line = head[-1].strip()

    # parse throughput
    _, throughput = [item.strip() for item in tp_line.split(":")]
    data = {"ops/sec(cum)": float(throughput)}

    # parse latency
    _, _, latencies = [item.strip() for item in latency_line.split(":")]
    latencies_with_units = latencies.split("/")
    p50, p95, p99 = [helper(latency_with_unit) for latency_with_unit in latencies_with_units]
    data.update({
        "p50(ms)": p50,
        "p95(ms)": p95,
        "p99(ms)": p99,
    })
    return data


def gather_data_from_raw_grpc_benchmark_logs(log_fpaths):
    acc = []

    for path in log_fpaths:

        with open(path, "r") as f:
            # read the first four lines of f
            print(path)
            head = f.readlines()[:4]

            try:
                datum = extract_data_grpc_benchmark(head)
                acc.append(datum)
            except BaseException as e:
                print("failed to extract data: {0}, err:{1}".format(path, e))
                return None, False

    final_datum = aggregate(acc)
    return final_datum, True

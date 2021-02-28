import argparse
import os
import sys

import csv_utils
import plot_utils
import sqlite_helper_object


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_file", type=str, required=True, help="trials.db")
    parser.add_argument("--csv_dir", type=str, required=True)
    parser.add_argument("--graph_dir", type=str, required=True)
    args = parser.parse_args()

    # Query data
    db = sqlite_helper_object.SQLiteHelperObject(args.db_file)
    db.connect()
    data = []
    c = db.c
    for row in c.execute("SELECT ops/sec(cum), p50(ms), p99(ms), skews "
                         "FROM trials_table "
                         "WHERE server_nodes=3"):
        data.append({
            "ops/sec(cum)": row[0],
            "p50(ms)": row[1],
            "p99(ms)": row[2],
            "skews": row[3],
        })
    db.close()

    # sort and write out data
    sorted(data, key=lambda point: point["skews"])
    csv_file = csv_utils.write_out_data(data,
                                        os.path.join(args.csv_dir, "dat.csv"))

    # graph data
    plot_utils.gnuplot("src/plot.gp", csv_file,
                       os.path.join(args.graph_dir, "p50_v_skew.png"),
                       os.path.join(args.graph_dir, "tp_v_skew.png"),
                       os.path.join(args.graph_dir, "p99_v_skew.png"))

    return 0


if __name__ == "__main__":
    sys.exit(main())

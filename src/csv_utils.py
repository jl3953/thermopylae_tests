import csv
import os


def read_in_data(filename):
  data = []
  if os.path.exists(filename):
    with open(filename, "r") as csvfile:
      reader = csv.DictReader(csvfile, delimiter='\t')
      for row in reader:
        for key in row:
          row[key] = float(row[key])

        data.append(dict(row))

  return data


def read_in_data_as_tuples(csv_fpath):
  header = None
  tuples = []

  with open(csv_fpath, "r") as f:
    reader = csv.reader(f, delimiter='\t')

    is_first_row = True
    for row in reader:
      if is_first_row:
        header = row
        is_first_row = False
      else:
        tuples.append([float(val) for val in row])

  return header, tuples


def write_out_data(data, filename):
  if len(data) <= 0:
    return ""

  with open(filename, "w") as csvfile:
    writer = csv.DictWriter(csvfile, delimiter='\t', fieldnames=data[0].keys())
    writer.writeheader()

    for datum in data:
      try:
        writer.writerow(datum)
      except BaseException:
        print("failed on {0}".format(datum))
        continue

  return filename

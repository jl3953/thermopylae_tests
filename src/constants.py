import os

COCKROACHDB_DIR = "/usr/local/temp/go/src/github.com/cockroachdb/cockroach"
TEST_PATH = os.path.join(COCKROACHDB_DIR, "tests")
TEST_CONFIG_PATH = os.path.join(TEST_PATH, "config")
TEST_SRC_PATH = os.path.join(TEST_PATH, "src")
SCRATCH_DIR = os.path.join(TEST_PATH, "scratch")

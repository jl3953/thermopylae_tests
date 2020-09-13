import os

COCKROACHDB_DIR = "/usr/local/temp/go/src/github.com/cockroachdb/cockroach"
TEST_PATH = "/usr/local/thermopylae_tests"
TEST_CONFIG_PATH = os.path.join(TEST_PATH, "config")
TEST_SRC_PATH = os.path.join(TEST_PATH, "src")
SCRATCH_DIR = os.path.join(TEST_PATH, "scratch")
CONFIG_FPATH_KEY = "config_fpath"

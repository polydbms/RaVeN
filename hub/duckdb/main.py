import argparse
from pathlib import Path

from hub.benchmarkrun.benchmark_run import BenchmarkRun
from hub.duckdb.init_duckdb import InitializeDuckDB
from hub.utils.fileio import FileIO


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment_conf", help="The config file to an experiment", required=True)
    parser.add_argument("--config",
                        help="Specify the path to the controller config file",
                        required=True)

    args = parser.parse_args()

    experiments: list[BenchmarkRun] = FileIO.read_experiments_config(args.experiment_conf, args.config)[0]

    init_db = InitializeDuckDB(experiments[0].host_params.controller_db_connection)

    init_db.setup_duckdb_tables()
    init_db.initialize_files(experiments)
    init_db.initialize_parameters(experiments)
    init_db.initialize_experiments(experiments, Path(args.experiment_conf).parts[-1])


if __name__ == "__main__":
    main()

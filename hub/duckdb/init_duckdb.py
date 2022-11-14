import argparse
from pathlib import Path

import duckdb

from hub.utils.fileio import FileIO


def setup_duckdb_tables(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
    create table if not exists files (
        filename varchar,
        type varchar,
        name varchar primary key
    )
    """)  # files_table

    conn.execute("""
    create table if not exists experiments (
        filename varchar primary key,
        notes varchar,
        raster_file varchar ,
        vector_file varchar ,
        foreign key (raster_file) references files(name),
        foreign key (vector_file) references files(name),
    )
    """)  # experiments_table

    conn.execute("""
    create table if not exists systems (
        name varchar primary key,
        comment varchar,
    )
    """)  # systems_table

    conn.execute("""
    CREATE SEQUENCE seq_experimentsid START 1;
    """)

    conn.execute("""
    create table if not exists benchmark_run (
        id int primary key default nextval('seq_experimentsid'),
        system varchar ,
        experiment varchar ,
        exec_start datetime,
        foreign key (system) references systems(name),
        foreign key (experiment) references experiments(filename),
    )
    """)  # benchmark_run_table

    conn.execute("""
    create table if not exists timings (
        run_id int,
        --marker varchar,
        --"timestamp" datetime,
        --event varchar,
        --stage varchar,
        system varchar,
        --dataset varchar,
        --comment varchar,
        controller_time datetime,
        primary key (run_id, controller_time),
        foreign key (system) references systems(name),
    )
    """)  # timings_table

    conn.execute("""
    create table if not exists resource_util (
        run_id int,
        timestamp datetime,
        primary key (run_id, timestamp)
    )
    """)  # resource_util_table

    conn.execute("""
    create table if not exists results (
        run_id int,
        feature_id varchar,
        primary key (run_id, feature_id)
    )
    """)  # results_table

    print("initialized tables")


def initialize_files(conn, experiments):
    experiment = experiments[list(experiments.keys())[0]]
    rasterfile = Path(experiment["raster"])
    conn.execute("insert into files (name, type, filename) values (?, ?, ?)",
                 [rasterfile.name, "raster", str(rasterfile)])

    vectorfile = Path(experiment["vector"])
    conn.execute("insert into files (name, type, filename) values (?, ?, ?)",
                 [vectorfile.name, "vector", str(vectorfile)])

    print("initialized files")


def initialize_experiments(conn, experiments, experiments_file):
    experiment = experiments[list(experiments.keys())[0]]
    rastername = Path(experiment["raster"]).name
    vectorname = Path(experiment["vector"]).name

    conn.execute("insert into experiments (filename, raster_file, vector_file) values (?, ?, ?)",
                 [experiments_file, rastername, vectorname])

    print("initialized experiments")


def initialize_systems(conn, experiments):
    conn.executemany("insert into systems (name) values (?)", [[e] for e in list(experiments.keys())])

    print("initialized systems")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment_conf", help="The config file to an experiment", required=True)

    args = parser.parse_args()

    experiments = FileIO.read_experiments_config(args.experiment_conf)

    with experiments[list(experiments.keys())[0]]["system"].db_connector.get_cursor() as conn:
        setup_duckdb_tables(conn)
        initialize_files(conn, experiments)
        initialize_systems(conn, experiments)
        initialize_experiments(conn, experiments, Path(args.experiment_conf).parts[-1])

    experiments[list(experiments.keys())[0]]["system"].db_connector.__del__()


if __name__ == "__main__":
    main()

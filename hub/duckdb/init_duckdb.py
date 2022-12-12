import pandas as pd

from hub.benchmarkrun.benchmark_run import BenchmarkRun
from hub.duckdb.submit_data import DuckDBConnector


class InitializeDuckDB:

    def __init__(self, connection: DuckDBConnector):
        self._connection = connection.get_cursor()

    def setup_duckdb_tables(self):
        self._connection.execute("""
        create table if not exists files (
            filename varchar,
            type varchar,
            name varchar primary key
        )
        """)  # files_table

        self._connection.execute("""
        create table if not exists experiments (
            filename varchar primary key,
            notes varchar,
            raster_file varchar ,
            vector_file varchar ,
            foreign key (raster_file) references files(name),
            foreign key (vector_file) references files(name)
        )
        """)  # experiments_table

        self._connection.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_parametersid START 1;
        """)

        self._connection.execute("""
        create table if not exists parameters (
            id int primary key default nextval('seq_parametersid'),
            system varchar,
            raster_target_format varchar,
            raster_target_crs varchar,
            raster_tile_size varchar,
            raster_depth ubigint,
            raster_resolution double,
            vectorize_type varchar,

            vector_target_format varchar,
            vector_target_crs varchar,
            vector_resolution double, 

            align_to_crs varchar,
            align_crs_at_stage varchar
        )
        """)  # systems_table

        self._connection.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_benchsetid START 1;
        """)

        self._connection.execute("""
        create table if not exists benchmark_set (
            id int primary key default nextval('seq_benchsetid'),
            experiment varchar ,
            exec_start timestamp,
            foreign key (experiment) references experiments(filename)
        )
        """)  # benchmark_run_table

        self._connection.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_benchrunid START 1;
        """)

        self._connection.execute("""
        create table if not exists benchmark_run (
            id int primary key default nextval('seq_benchrunid'),
            parameters int,
            benchmark_set int,
            iteration int,
            foreign key (parameters) references parameters(id),
            foreign key (benchmark_set) references benchmark_set(id)
        )
        """)  # benchmark_run_table

        self._connection.execute("""
        create table if not exists timings (
            run_id int,
            marker varchar,
            "timestamp" datetime,
            event varchar,
            stage varchar,
            dataset varchar,
            comment varchar,
            controller_time datetime,
            primary key (run_id, controller_time),
            foreign key (run_id) references benchmark_run(id)
        )
        """)  # timings_table

        self._connection.execute("""
        create table if not exists resource_util (
            run_id int,
            timestamp_host datetime,
            ID varchar,
            Name varchar,
            CPUUsage double,
            MemUsage ubigint,
            MemLimit ubigint,
            NetIO_in ubigint,
            NetIO_out ubigint,
            BlockIO_in ubigint,
            BlockIO_out ubigint,
            PIDs uinteger,
            stage varchar,
            primary key (run_id, stage, timestamp_host)
        )
        """)  # resource_util_table

        self._connection.execute("""
        create table if not exists results (
            run_id int,
            warm_start_no int,
            result_file varchar,
            primary key (run_id, warm_start_no)
        )
        """)  # results_table

        print("initialized tables")

    def initialize_files(self, experiments: list[BenchmarkRun]):
        experiment = experiments[0]
        rasterfile = experiment.raster
        raster_ingest = self._connection.execute(
            "insert into files (name, type, filename) select ? as name, ? as type, ? as filename where ? not in (select name from files) returning *",
            [rasterfile.name, "raster", str(rasterfile), rasterfile.name]).fetchone()[0]

        vectorfile = experiment.vector
        vector_ingest = self._connection.execute(
            "insert into files (name, type, filename) select ? as name, ? as type, ? as filename where ? not in (select name from files) returning *",
            [vectorfile.name, "vector", str(vectorfile), vectorfile.name]).fetchone()[0]

        print(
            f"initialized files, added raster: {True if raster_ingest else False}, vector: {True if vector_ingest else False}")

    def initialize_experiments(self, experiments: list[BenchmarkRun], experiments_file):
        experiment = experiments[0]
        rastername = experiment.raster.name
        vectorname = experiment.vector.name

        experiment_add = self._connection.execute(
            "insert into experiments (filename, raster_file, vector_file) select ? as filename, ? as raster_file, ? as vector_file where ? not in (select filename from experiments)",
            [experiments_file, rastername, vectorname, experiments_file]).fetchone()[0]

        print(f"initialized experiments, added new experiment: {True if experiment_add else False}")

    def initialize_parameters(self, experiments: list[BenchmarkRun]):
        # max_id = conn.execute("SELECT max(id) from parameters").fetchone()
        # max_id = int(max_id[0]) if max_id[0] else 1
        old_exp = self._connection.execute("select * from parameters").fetch_df().set_index("id")
        new_exp_df = pd.DataFrame([e.benchmark_params.__dict__ for e in experiments]).fillna('')
        new_exp_cleaned = self._connection.execute("select * from new_exp_df").fetch_df()
        cleaned = new_exp_cleaned.merge(old_exp, how="left", indicator=True)
        cleaned = cleaned[cleaned["_merge"] == 'left_only']
        cleaned.drop(columns=["_merge"], axis=1, inplace=True)
        max_id = len(old_exp)
        cleaned.insert(0, "id", range(max_id + 1, max_id + len(cleaned) + 1))
        self._connection.execute("INSERT INTO parameters SELECT * FROM cleaned")
        # conn.executemany("insert into parameters values (?)", [e.benchmark_params.value_array() for e in experiments])

        print(
            f"initialized parameters, added {len(cleaned)} new, "
            f"now {self._connection.execute('SELECT count(*) from parameters').fetchone()[0]} exist")

    def __del__(self):
        self._connection.close()

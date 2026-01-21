import argparse
from functools import wraps
from pathlib import Path

import pandas as pd

from hub.raven import Setup
import geopandas as gpd



class Raven:
    @staticmethod
    def cli_main():
        """
        the main method containing the CLI
        :return:
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("command", help="Use either start, optimize, clean, or eval.")
        parser.add_argument("--system", help="Specify which system should be benchmarked")
        parser.add_argument("--config",
                            help="Specify the path to the controller config file",
                            required=True)
        parser.add_argument("--experiment",
                            help="Specify the path to the experiment definition file",
                            required=False,
                            action="append")
        parser.add_argument("--experiment-list",
                            help="Specify a list of experiments from a file",
                            required=False,
                            action="append",
                            type=argparse.FileType("r", encoding="utf-8")
                            )
        parser.add_argument("--postcleanup",
                            help="Whether to run a cleanup after running the benchmark. Only works together with '--system <system>'",
                            action=argparse.BooleanOptionalAction,
                            default=True)
        parser.add_argument("--singlerun",
                            help="Whether to run only one the first experiment. Only works together with '--system <system>'",
                            action=argparse.BooleanOptionalAction,
                            default=False)
        parser.add_argument("--eval",
                            help="Whether to run the evaluation",
                            action=argparse.BooleanOptionalAction,
                            default=True)
        parser.add_argument("--resultsfile",
                            help="specify files for running the eval",
                            nargs="+")
        parser.add_argument("--evalbase",
                            help="specify which system should act as the baseline in eval mode. If none is specified, the lexicographically first string is chosen.")
        parser.add_argument("--evalfolder",
                            help="specify the name of the output eval folder.")
        parser.add_argument("--output_format",
                            help="specify the output format for the evaluation. Default is csv",
                            default="csv")
        parser.add_argument("--stop_at_preprocess",
                            help="stop the benchmark after preprocessing",
                            action=argparse.BooleanOptionalAction,
                            default=False)
        parser.add_argument("--cleanup_each_optimize",
                            help="whether to clean up after running the benchmark",
                            action=argparse.BooleanOptionalAction)
        parser.add_argument("--dryrun",
                            help="whether to only print the commands without executing them",
                            action=argparse.BooleanOptionalAction,
                            default=False)

        args = parser.parse_args()
        if not args.experiment and not args.experiment_list:
            parser.error("At least one experiment must be specified with --experiment or --experiment-list.")

        exp_dict = {}

        if args.experiment_list:
            if args.experiment:
                exp_dict["no_list"] = args.experiment

            for file in args.experiment_list:
                exp = [Path(line.strip()) for line in file if line.strip() and not line.startswith("#")]

                exp_abs = []
                for e in exp:
                    if not e.is_absolute():
                        e = Path(file.name).parent.joinpath(e)
                    exp_abs.append(e)

                exp_dict[file.name] = exp_abs

            args.experiment = [item for sublist in exp_dict.values() for item in sublist]

        print(args)
        setup = Setup()
        match args.command:
            case "start":
                for experiment_file_name in args.experiment:
                    result_files, vector_file_location, join_attrs = setup.benchmark(experiment_file_name, args.config, args.system,
                                                                         args.postcleanup,
                                                                         args.singlerun, args.stop_at_preprocess)

                    if len(result_files) >= 1:
                        match args.output_format:
                            case "gpkg":
                                for f in result_files:
                                    try:
                                        result = pd.read_csv(f)
                                        base_vector = gpd.read_file(vector_file_location)
                                        base_vector.merge(result, how="right", on=join_attrs).to_file(f.with_suffix(".gpkg"), driver="GPKG")
                                    except Exception as e:
                                        print(f"maybe the resultset was empty?: {e}")

                        if args.eval and len(result_files) >= 2:
                            setup.evaluate(args.config, result_files, args.evalbase)
            case "optimize":
                for experiment_file_name in args.experiment:
                    setup.optimize(experiment_file_name, args.config)

                    if args.cleanup_each_optimize:
                        setup.clean(args.config)

                if args.postcleanup:
                    setup.clean(args.config)
            case "optimize_multiple_bench":
                for exp_list, experiments in exp_dict.items():

                    print("Running experiment group {} with experiments: {}".format(exp_list, experiments))

                    last_run_id = -2
                    for experiment_file_name in experiments:
                        last_run_id = setup.optimize(str(experiment_file_name), args.config, last_run_id, exp_group=exp_list, dry_run=args.dryrun)

                    if not args.postcleanup:
                        return

                    setup.clean(args.config)

                    # isolated run

                    for experiment_file_name in experiments:
                        setup.optimize(str(experiment_file_name), args.config, last_run_id=-4, exp_group=exp_list, dry_run=args.dryrun)

                        setup.clean(args.config)
            case "clean":
                setup.clean(args.config)
            case "eval":
                setup.evaluate(args.config, list(map(lambda f: Path(f), args.resultsfile)), args.evalbase, args.evalfolder)


if __name__ == "__main__":
    Raven.cli_main()

import argparse
from pathlib import Path

from hub.raven import Setup


class Raven:
    @staticmethod
    def cli_main():
        """
        the main method containing the CLI
        :return:
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("command", help="Use either start, clean, or eval.")
        parser.add_argument("--system", help="Specify which system should be benchmarked")
        parser.add_argument("--config",
                            help="Specify the path to the controller config file",
                            required=True)
        parser.add_argument("--experiment",
                            help="Specify the path to the experiment definition file",
                            required=True,
                            action="append")
        parser.add_argument("--postcleanup",
                            help="Whether to run a cleanup after running the benchmark. Only works together with '--system <system>'",
                            action=argparse.BooleanOptionalAction,
                            default=True)
        parser.add_argument("--singlerun",
                            help="Whether to run only one the first experiment. Only works together with '--system <system>'",
                            action=argparse.BooleanOptionalAction,
                            default=True)
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

        args = parser.parse_args()
        print(args)
        setup = Setup()
        if args.command == "start":
            for experiment_file_name in args.experiment:
                result_files = setup.benchmark(experiment_file_name, args.config, args.system, args.postcleanup,
                                               args.singlerun)

                if len(result_files) > 1 and args.eval:
                    setup.evaluate(args.config, result_files, args.evalbase)
        if args.command == "clean":
            setup.clean(args.config)
        if args.command == "eval":
            setup.evaluate(args.config, list(map(lambda f: Path(f), args.resultsfile)), args.evalbase, args.evalfolder)


if __name__ == "__main__":
    Raven.cli_main()

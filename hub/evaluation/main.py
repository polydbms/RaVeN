from datetime import datetime
from pathlib import Path

import pandas as pd

from hub.benchmarkrun.host_params import HostParameters
from hub.utils.system import System


class Evaluator:
    def __init__(self, systems_list: list[System], result_files: list[Path], host_params: HostParameters) -> None:
        self.host_params = host_params
        self.system_list = [s.name for s in systems_list]
        self.result_files = result_files
        self.timestring = datetime.now().strftime('%Y%m%d-%H%M%S')
        self.eval_output_folder = self.host_params.controller_result_base_folder.joinpath(f"eval_{self.timestring}")
        self.eval_output_folder.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def __read_result(file):
        return pd.read_csv(file, sep=",")

    @staticmethod
    def __get_columns(df):
        return list(df.columns)[1:]

    @staticmethod
    def __get_index(df):
        """Assumption is that the first column is the index"""
        columns = list(df.columns)
        return columns[0]

    def __get_base(self):
        base_system = self.system_list[0]
        path = next(f for f in self.result_files if base_system in f.stem)
        df = self.__read_result(path)
        index = self.__get_index(df)
        df = df.sort_values(by=[index])
        df = df.reset_index(drop=True)
        columns = self.__get_columns(df)
        return df, index, columns, base_system

    def __get_secondary(self):
        secondary_system = self.system_list[1:]
        df_list = []
        for system in secondary_system:
            path = next(f for f in self.result_files if system in f.stem)
            df = self.__read_result(path)
            index = self.__get_index(df)
            df = df.sort_values(by=[index])
            df = df.reset_index(drop=True)
            columns = self.__get_columns(df)
            df_list.append((df, index, columns, system))
        return df_list

    @staticmethod
    def get_diff(df, base_columns, system_columns):
        column_pair = zip(base_columns, system_columns)
        average_diff = []
        for pair in column_pair:
            try:
                if pair[0] == pair[1]:
                    df[f"diff_{pair[1]}"] = (
                            100
                            * abs(df[f"{pair[0]}_x"] - df[f"{pair[1]}_y"])
                            / ((df[f"{pair[0]}_x"] + df[f"{pair[1]}_y"]) / 2)
                    )
                else:
                    df[f"diff_{pair[1]}"] = (
                            100
                            * abs(df[pair[0]] - df[pair[1]])
                            / ((df[pair[0]] + df[pair[1]]) / 2)
                    )
            except TypeError as e:
                df[f"diff_{pair[1]}"] = 100
            average_diff.append((pair[1], df[f"diff_{pair[1]}"].mean()))
        return df, average_diff

    def get_accuracy(self):
        base_df, base_index, base_columns, base_system = self.__get_base()
        secondary = self.__get_secondary()
        for system_df, system_index, system_columns, system in secondary:
            out = pd.DataFrame()
            out = pd.merge(base_df, system_df, on=base_index)
            out.drop_duplicates(subset=[base_index], keep="last", inplace=True)
            out, average_diff = self.get_diff(out, base_columns, system_columns)
            out.to_csv(
                self.eval_output_folder.joinpath(f"{base_system}_vs_{system}.{self.timestring}.csv"))
            accuracy = pd.DataFrame(average_diff, columns=["Feature", "Accuracy"])
            accuracy.to_csv(self.eval_output_folder
                            .joinpath(f"{base_system}_vs_{system}_accuracy.{self.timestring}.csv"))

import time
import pandas as pd


def measure_time(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        with open("out.log", "a") as f:
            f.write("%r;%2.2f\n" % (method.__qualname__, (te - ts) * 1000))
        if "log_time" in kw:
            name = kw.get("log_name", method.__qualname__.upper())
            kw["log_time"][name] = int((te - ts) * 1000)
        else:
            print("%r  %2.2f ms" % (method.__qualname__, (te - ts) * 1000))
        return result

    return timed


class Evaluator:
    def __init__(self, systems_list) -> None:
        self.system_list = systems_list
        self.data_path = "~/results_"

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
        path = f"{self.data_path}{base_system}.csv"
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
            path = f"{self.data_path}{system}.csv"
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
            out.to_csv(f"~/{base_system}_vs_{system}.csv")
            accuracy = pd.DataFrame(average_diff, columns=["Feature", "Accuracy"])
            accuracy.to_csv(f"~/{base_system}_vs_{system}_accuracy.csv")

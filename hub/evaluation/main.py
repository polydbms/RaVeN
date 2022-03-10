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


def compare(file1, file2, sort_key="names", arg="avg"):
    df1 = pd.read_csv(file1, sep=",")
    df2 = pd.read_csv(file2, sep=",")
    file1 = file1.split("/")[0]
    file2 = file2.split("/")[0]
    out = pd.DataFrame()
    df1 = df1.sort_values(by=[sort_key])
    df1 = df1.reset_index(drop=True)
    df2 = df2.sort_values(by=[sort_key])
    df2 = df2.reset_index(drop=True)
    out = pd.merge(df1, df2, on=sort_key)
    try:
        out["diff"] = 100 * abs(out[f"{arg}_x"] - out[f"{arg}_y"]) / out[f"{arg}_x"]
    except TypeError as e:
        out["diff"] = 100
    out[["names", "diff"]].to_csv(f"comparisons/{file1}_vs_{file2}_{arg}.csv")
    average_diff = out["diff"].mean()
    print(f"{file1}_vs_{file2}_{arg}: {average_diff}")

import time


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

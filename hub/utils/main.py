import time
from hub.evaluation.measure_time import measure_time


# measure time (logger={log_time:{}})
## deploy
## preprocess data (preprocessor)
## send data (ingestor)
## ingest data (ingestor)
## run query (executor)
# get results (evaluator)
# compare (evaluator)


@measure_time
def deploy(**kwargs):
    time.sleep(1)
    print("deploy")


@measure_time
def preprocess(**kwargs):
    time.sleep(2)
    print("preprocess")


@measure_time
def send(**kwargs):
    time.sleep(1)
    print("send")


@measure_time
def ingest(**kwargs):
    time.sleep(3)
    print("ingest")


@measure_time
def run(**kwargs):
    time.sleep(5)
    print("run")


logger = {}
deploy(log_time=logger)
preprocess(log_time=logger)
send(log_time=logger)
ingest(log_time=logger)
run(log_time=logger)

print(logger)

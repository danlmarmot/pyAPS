from __future__ import print_function

import os
import time
from datetime import datetime

from apscheduler.scheduler import Scheduler
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from multiprocessing import Process, Queue


class settings:
    pass


settings.MONGO_HOST = "127.0.0.1"
settings.MONGO_PORT = 27017
settings.MONGO_DB = "tasks"
settings.MONGO_LOCKS = "locks"
settings.MONGO_RUNS = "runs"

# At startup, print some stuff
print("Starting at " + datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
parentPid = str(os.getpid())
print("Pid of this process is " + parentPid)
startTime = time.time()

try:
    conn = MongoClient(settings.MONGO_HOST, settings.MONGO_PORT)
except Exception, ConnectionFailure:
    raise ConnectionFailure

# Get a connection to the locks collection
# Also, ensure the field in locks collection is indexed with a TTL index, and taskname is unique
locks_coll = conn[settings.MONGO_DB][settings.MONGO_LOCKS]
locks_coll.create_index("insertTime", expireAfterSeconds=30)
locks_coll.create_index("uniqueTask", unique=True)

# Get a separate connection to where we store the results of each run
runs_coll = conn[settings.MONGO_DB][settings.MONGO_RUNS]

sched = Scheduler()

# Schedule to be called every seven seconds
# Seven is chosen just because it's a prime number
@sched.interval_schedule(seconds=7)
def schedule_task_1():
    taskSecs = 5
    taskName = "task1"

    record = {
        "insertTime": datetime.utcnow(),
        "uniqueTask": taskName,
        "pid": parentPid
    }

    try:
        locks_coll.insert(record)
        print("Added record, got lock, running task. Elapsed secs: " + str(int(time.time() - startTime)))
        q = Queue()
        p = Process(target=task1, args=(q, taskSecs))
        p.start()

        result = q.get()
        print("Task run.  Results: \n" + result)

    except DuplicateKeyError:
        print(
            "Record exists, implying task is already running. Elapsed secs: " + str(int(time.time() - startTime)))


sched.start()


def task1(q, taskSecs=5):
    startTime = datetime.utcnow()
    pid = str(os.getpid())

    r = "    PID is " + pid
    r += "\n    Started task at  " + startTime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # Simulate some work by sleeping for awhile
    for x in range(1, int(taskSecs + 1)):
        time.sleep(1)

    finishTime = datetime.utcnow()
    timeDiff = str(finishTime - startTime)

    # Add a record to Mongo for the work done.
    record = {
        "pid": pid,
        "parentPid": parentPid,
        "startTime": startTime,
        "finishTime": finishTime,
        "taskTime": timeDiff
    }
    runs_coll.insert(record)

    r += "\n    Finished task at " + finishTime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    r += "\n    Task time is     " + timeDiff
    q.put(r)


while True:
    pass

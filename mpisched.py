import time
from pyscheduler.MPIParallelScheduler import MPIParallelScheduler
def sleep(this):
    print (this, "is going to sleep")
    time.sleep(this)
    print (this, "has woken up")
    return this

def echo(this):
    return this

def add_tasks(scheduler, test_function):
    scheduler.add_task(task_name = "1", dependencies = ["2","3"], description ="",target_function = test_function ,function_kwargs={"this":1})
    scheduler.add_task(task_name = "2", dependencies = ["4"], description ="",target_function = test_function ,function_kwargs={"this":2})
    scheduler.add_task(task_name = "3", dependencies = ["5","6"], description ="",target_function = test_function ,function_kwargs={"this":3})
    scheduler.add_task(task_name = "4", dependencies = [], description ="",target_function = test_function ,function_kwargs={"this":4})
    scheduler.add_task(task_name = "5", dependencies = [], description ="",target_function = test_function ,function_kwargs={"this":5})
    scheduler.add_task(task_name = "6", dependencies = [], description ="",target_function = test_function ,function_kwargs={"this":6})
    scheduler.add_task(task_name = "7", dependencies = [], description ="",target_function = test_function ,function_kwargs={"this":7})
    scheduler.add_task(task_name = "8", dependencies = ["9"], description ="",target_function = test_function ,function_kwargs={"this":8})
    scheduler.add_task(task_name = "9", dependencies = [], description ="",target_function = test_function ,function_kwargs={"this":9})


parallel = MPIParallelScheduler()
add_tasks(parallel, sleep)
results = parallel.run()
print(results)

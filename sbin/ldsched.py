#!/usr/bin/env python

"""
This is a scheduler for logdiver. Currently the code supports - slurm and torque. Two most widely suppported rm's on
HPC.
"""

# enable local module importing
import os
import sys 

# load all modules
import re
import time
#from mpi4py import MPI
from Generator import Program
from Generator import Generator
from pprint import  pprint
from LDParser import parse_program
import argparse
from pyscheduler.MPIParallelScheduler import MPIParallelScheduler

DEBUG = False

def debugWrite(msg):
    if DEBUG:
        ofh = open("run.log" + msg , 'w')
        ofh.close()

def main(programParams):
    p = Program(
        programParams["name"],
        programParams["executable"],
        len(programParams["args"].keys()),
        programParams["isNamedParameters"],
        programParams["args"]
    )
    print("program name", programParams["name"])
    print("executable", programParams["executable"])
    tq = Generator(p).taskQueue
    # define the scheduler type
    parallel = MPIParallelScheduler()
    taskID = 0
    while (tq.qsize()>0):
        work = tq.get()
        parallel.add_task(task_name = str(taskID), dependencies = [], description ="", target_function = work.start, function_kwargs={})
        taskID += 1
    results = parallel.run()
    print(results)
    # a unit of work 
    # work = tq.get()

if __name__ =="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pcfile", dest = "programConf", required = True)
    args = parser.parse_args()
    programParams = parse_program(args.programConf)
    main(programParams)

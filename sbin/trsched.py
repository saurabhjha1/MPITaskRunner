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
from mpi4py import MPI
from Generator import Program
from Generator import Generator
from pprint import  pprint
from LDParser import parse_program
import argparse 

WORKTAG = 1
DIETAG = 0
KILLTAG = 2
RESULTTAG = 3
ACKTAG = 4
NACKTAG = 5
DEBUG = True

def debugWrite(msg):
    if DEBUG:
        ofh = open("/u/sciteam/sjha/scratch/logs/" + msg , 'w')
        ofh.close()

def master(comm, programParams):
    p = Program(
        programParams["name"],
        programParams["executable"],
        len(programParams["args"].keys()),
        programParams["isNamedParameters"],
        programParams["args"]
    )
    print("program name", programParams["name"])
    print("executable", programParams["executable"])
    
    g = Generator(p)
    num_procs = min(comm.Get_size(), g.taskQueue.qsize())
    status = MPI.Status()

    print("CommSize = %d, Num tasks = %d, required Processes = %d" % (comm.Get_size(), g.taskQueue.qsize(), num_procs)) 
    # edge case when numtasks are greater than num tasks
    if (comm.Get_size() - g.taskQueue.qsize() > 0) :
        print("killing extra processes = %d" % (comm.Get_size() - g.taskQueue.qsize()))
        print("should not reach here in general. More of an exception than a rule")
        for rank in xrange(g.taskQueue.qsize(), comm.Get_size()):
            print("invalid rank = %d" % rank)
            comm.send(0, dest=rank, tag=KILLTAG)
    debugWrite("files_%d_size_%d" % (g.taskQueue.qsize(), num_procs))
    # create mpi status for each for num procs
    work_status = [ False for i in xrange(0, num_procs)]
    processingTimes = {}  # contains rank -> tid -> (startime, endtime)
    for i in xrange(0, num_procs):
        processingTimes[i] = {}
    currTid = [ -1 for i in xrange(0, num_procs)] # currenttid that each rank is processing 
    while True:
        #time.sleep(1)
        #Seed the slaves, send one unit of work to each slave (rank)
        for rank in xrange(1, num_procs):
            # make sure there is task in queue and that particular is not a;read processing and finally 
            # a;so make sure thet isend does not have more than one outstanding outgoing message
            if g.taskQueue.qsize() > 0 and work_status[rank] == False and sum(work_status) <= num_procs:
                #mark the the mpi thread to be active in processing task
                work = g.taskQueue.get()
                work_status[rank] = True
                comm.isend(work, dest=rank, tag=WORKTAG)
                debugWrite("server_rank_%d_send_%s" % (rank, str(time.time())))
                taskTid = work.result.taskId
                currTid[rank] = taskTid
                if taskTid not in processingTimes[rank].keys():
                    processingTimes[rank][currTid[rank]] = [time.time(), time.time()]
            if work_status[rank] == True:
                tStatus = MPI.Status()
                comm.Iprobe(rank, RESULTTAG, tStatus)
                if tStatus.Get_tag() == RESULTTAG:
                    debugWrite("server_rank_%d_iprobe_%s" %(rank, str(time.time())))
                    processingTimes[rank][currTid[rank]][1] = time.time() - processingTimes[rank][currTid[rank]][1] 
                    try:
                        result = comm.recv(source = rank, tag = RESULTTAG, status = tStatus)
                        if result != None:
                            print("Tid = %d result: cmd: %s, stdout: %s, exit status : %d" % (rank, result.runCommandName, result.stdOutErrLines, result.retval))
                            debugWrite("server_rank_%d_recvd_%s" %(rank, str(time.time())))
                        work_status[rank] = False
                        comm.send(True, dest=rank, tag=ACKTAG)
                    except:
                        print("result corrupted, will try again")
        if sum(work_status) == 0 and g.taskQueue.qsize() == 0:
            break
    for rank in xrange(1, num_procs):
        debugWrite("client_rank_%d_KILL_SIG" % rank)
        req = comm.send(0, dest=rank, tag=DIETAG)
    pprint(processingTimes)    

def slave(comm):
    my_rank = comm.Get_rank()
    while True:
        status = MPI.Status()
        work = None
        result = None
        # Receive a message from the master
        try:
            work = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            # Check the tag of the received message
            if status.Get_tag() == KILLTAG: 
                print("Rank = %d, server forced kill" % my_rank)
                break

            if status.Get_tag() == DIETAG: 
                debugWrite("Rank_%d_quiting_%s" % (my_rank, str(time.time())))
                break

            print("Tid = %d start: doing task %d, cmd : %s" %(my_rank, work.result.taskId, work.runCommandName))
            # Do the work
            debugWrite("client_rank_%d_ws_%s" % (my_rank, str(time.time())))
            result = work.start()
            comm.send(result, dest=0, tag=RESULTTAG)
            while True: # make sure the master recieved the result
                ackmsg = comm.recv(source = 0, tag = ACKTAG, status=status)
                if ackmsg == True:
                    break
        except:
             print("MESSAGE CORRUPTED")
             comm.send(None, dest=0, tag=RESULTTAG)
    debugWrite("CLIENT_%d_EXIT" % my_rank)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pcfile", dest = "programConf", required = True)
    args = parser.parse_args()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    if (size < 2):
        if (rank == 0):
            print("MPI COMM SIZE SHOULD BE ATLEAST 2")
        exit(1)
    # assign master slaves and define their work
    if rank == 0:
        programParams = parse_program(args.programConf)
        print("Done parsing")
    comm.Barrier()
    if rank == 0:
        master(comm, programParams)
    else:
        slave(comm)


if __name__ == "__main__":
    main()

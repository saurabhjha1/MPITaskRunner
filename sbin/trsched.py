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
from pprint import pprint
from LDParser import parse_program
import argparse

MASTER_RANK = 0
ANYTAG = 0

DEBUG = False


def debug_write(msg):
    if DEBUG:
        ofh = open("/u/sciteam/sjha/scratch/logs/" + msg, 'w')
        ofh.close()


def send_to_slave(comm, slave_rank, msg):
    comm.send(msg, dest = slave_rank, tag = ANYTAG)


def recv_from_slave(comm, slave_rank):
    return comm.recv(source = slave_rank, tag = ANYTAG)


def send_from_slave(comm, msg):
    comm.send(msg, dest = MASTER_RANK, tag = ANYTAG)


def recv_from_master(comm):
    return comm.recv(source = MASTER_RANK, tag = ANYTAG)


def master(comm, programParams):
    p = Program(
        programParams["name"],
        programParams["executable"],
        len(programParams["args"].keys()),
        programParams["isNamedParameters"],
        programParams["args"]
    )

    g = Generator(p)
    num_procs = min(comm.Get_size(), len(g.taskQueue) + 1)

    print("CommSize = %d, Num tasks = %d, required Processes = %d" % (comm.Get_size(), len(g.taskQueue), num_procs))
    # edge case when numtasks are greater than num tasks
    if comm.Get_size() - (len(g.taskQueue) + 1) > 0: #master just dispatches so add dispatching as a task
        print("killing extra processes = %d" % (comm.Get_size() - len(g.taskQueue)))
        print("should not reach here in general. More of an exception than a rule")
        for slave_rank in range(len(g.taskQueue) + 1,  comm.Get_size()):
            send_to_slave(comm, slave_rank, [None, "DIE"])
            msg = recv_from_slave(comm, slave_rank)
            if msg[1] != "DIE":
                print("Error occurred while killing in slave %d" % slave_rank)

    # debugWrite("files_%d_size_%d" % (len(g.taskQueue), num_procs))
    # create mpi status for each for num procs
    work_status = [False] * num_procs
    processing_times = {}  # contains rank -> tid -> (startime, endtime)
    for i in range(1, num_procs):
        processing_times[i] = {}
    curr_tid = [-1] * num_procs  # currenttid that each rank is processing
    while True:
        if sum(work_status) == 0 and len(g.taskQueue) == 0:
            break
        # Seed the slaves, send one unit of work to each slave (rank)
        for slave_rank in range(1, num_procs):
            # make sure there is task in queue and that particular is not a;read processing and finally
            # also make sure that isend does not have more than one outstanding outgoing message
            # send part for the task
            if len(g.taskQueue) > 0 and not work_status[slave_rank]:
                work_status[slave_rank] = True
                # mark the the mpi thread to be active in processing task
                work = g.taskQueue.pop(0)
                curr_tid[slave_rank] = work.result.taskId
                print("master is sending %s to %d" % (work.runCommandName, slave_rank))
                send_to_slave(comm, slave_rank, [work, "WORK"])
                debug_write("server_rank_%d_send_%s" % (slave_rank, str(time.time())))
                if work.result.taskId not in processing_times[slave_rank].keys():
                    processing_times[slave_rank][curr_tid[slave_rank]] = [time.time(), time.time()]
            # recv part
            elif work_status[slave_rank]:
                tStatus = MPI.Status()
                comm.Iprobe(slave_rank, 0, tStatus)
                if tStatus.Get_tag() == 0:
                    processing_times[slave_rank][curr_tid[slave_rank]][1] = time.time() - processing_times[slave_rank][curr_tid[slave_rank]][1]
                    work_status[slave_rank] = False
                    try:
                        result = recv_from_slave(comm, slave_rank)
                        if result[1] == "WORK":
                            print("Tid = %d result: cmd: %s, stdout: %s, exit status : %d" % (
                                slave_rank, result[0].runCommandName, result[0].stdOutErrLines, result[0].retval))
                            debug_write("server_rank_%d_recvd_%s" % (slave_rank, str(time.time())))
                    except:
                        print("result corrupted, will try again")

    for slave_rank in range(1, num_procs):
        send_to_slave(comm, slave_rank, [None, "DIE"])
    pprint(processing_times)


def slave(comm):
    my_rank = comm.Get_rank()
    while True:
        try:
            msg = recv_from_master(comm)
            if msg[1] == "DIE":
                send_from_slave(comm, [None, "DIE"])
                return
            elif msg[1] == "WORK":
                work = msg[0]
                print("Tid = %d start: doing task %d, cmd : %s" % (my_rank, work.result.taskId, work.runCommandName))
                # Do the work
                debug_write("client_rank_%d_ws_%s" % (my_rank, str(time.time())))
                result = work.start()
                send_from_slave(comm, [result, "WORK"])

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print("MESSAGE CORRUPTED")
            send_from_slave(comm, [None, "ERROR"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pcfile", dest="programConf", required=True)
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

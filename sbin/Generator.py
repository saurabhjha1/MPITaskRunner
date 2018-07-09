#!/usr/bin/env python
# enable local dir module imports
import os 
import sys

#from Queue import Queue
import subprocess
import itertools
from pprint import pprint
from LDParser import parse_program
import argparse

OSKILLED = -1000

class Program:
    """
    Program is code that takes various parameters. Parameters of a code in a task set can be static or varying. The class
    reads a user defined configuration file and sets the parameter scope.
    """
    name = ""
    numParameters = 0 # number of parameters this code will take
    isNamedParameters = False # are parameters are named such as do they expect --<parameter name> = value  or -<parameter name> = <value>"
    parameters = {}   # dictionary parameter id to array of all values it can take
    executable = ""
    
    def __init__(self, name, executable, numParameters, isNamedParameters, parameters):
        # todo read from conf file for now lets just set the values manually
        self.name = name
        self.executable = executable
        self.numParameters = numParameters
        self.isNamedParameters = isNamedParameters
        self.parameters = parameters

class TaskStatus(object):
    INITIALIZED = 0
    QUEUED = 1
    RUNNING = 2
    FINISHED = 3
    ABORT = 4
    FAILED = 5
    SUCCESS = 6

class Result:
    taskId = None
    stdOutErrLines = None
    retval = None
    def __init__(self, taskId):
        self.taskId = taskId
        self.stdOutErrLines = ""
        self.retval = -1
        self.status = TaskStatus.INITIALIZED

class Task:
    """
    Task essentially is a specific instantiation of the proram. i.e. all the program assignments are completely specified
    """
    runCommandName = ""
    numTries = 0
    result = None

    def __init__(self, executableName, args, taskId):
        self.result = Result(taskId)
        parameterString = executableName + " "
        numArgs = len(args)
        #print(numArgs)
        for argNum in range(0,numArgs):
            arg = args[argNum]
            numParams = len(arg)
            for paramNum in range(0,numParams):
                parameterString += " " + arg[paramNum]
        self.runCommandName = parameterString
        self.result.runCommandName = parameterString
        # todo change the runCommandName
        self.result.status = TaskStatus.INITIALIZED
        self.numTries = 0


    def enqueue(self):
        self.status = TaskStatus.QUEUED

    def start(self):
        self.status = TaskStatus.RUNNING
        p = None
        try:
            p = subprocess.Popen(self.runCommandName, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            self.result.retval = p.wait()
            self.result.stdOutErrLines =  p.stdout.readlines()
            self.result.status = TaskStatus.FINISHED
            if (self.result.retval != 0):
                self.result.status = TaskStatus.FAILED
            else:
                self.result.status = TaskStatus.SUCCESS
        except:
            self.result.retval = -OSKILLED
            self.result.stdOutErrLines = "KILLED BY OS"
            self.result.status = TaskStatus.ABORT
        return self.result
        # todo: send message to master of completing this job

class Generator:
    """
    The goal of the generator is to take in Program definition and vary the parameters to generate tasks
    """
    taskQueue = []
    taskList = []

    def __init__(self, program):
        formattedArguments = []
        if program.isNamedParameters == True:
            programParameterValues = [ program.parameters[i][1] for i in range(0, len(program.parameters.keys())) ]
            argumentSpace = itertools.product(*programParameterValues)
            argHeader = [program.parameters[i][0] for i in range(0, len(program.parameters.keys()))]
            numArgs = len(program.parameters.keys())
            for argSet in argumentSpace:
                newArgSet = []
                argNum = 0
                for arg in argSet:
                    newArgSet.append([argHeader[argNum], arg])
                    argNum += 1
                formattedArguments.append(newArgSet)
        else:
            programParameterValues = [ program.parameters[i][1] for i in range(0, len(program.parameters.keys())) ]
            argumentSpace = itertools.product(*programParameterValues)
            numArgs = len(program.parameters.keys())
            for argSet in argumentSpace:
                newArgSet = []
                for arg in argSet:
                    #print(arg)
                    newArgSet.append([arg])
                formattedArguments.append(newArgSet)

        # create task
        taskID = 0
        for argSet in formattedArguments:
            t  = Task(program.executable, argSet, taskID)
            taskID += 1
            self.taskList.append(t)
            self.taskQueue.append(t)
            t.enqueue()
        print("Total tasks queued = %d" % len(self.taskQueue))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pcfile", dest="programConf", required = True)
    args = parser.parse_args()
    programParams = parse_program(args.programConf)
    p = Program(
        programParams["name"],
        programParams["executable"],
        len(programParams["args"].keys()),
        programParams["isNamedParameters"],
        programParams["args"]
    )
    
    pprint(programParams)
    g = Generator(p)
    while(len(g.taskQueue) > 0):
        print(g.taskQueue[0].runCommandName)
        g.taskQueue.pop(0)
if __name__ == "__main__":
    main()


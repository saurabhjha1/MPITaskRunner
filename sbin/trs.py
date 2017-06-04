#!/usr/bin/env python

#enable local module importing
import os
import sys
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)


from pprint import pprint
from LDParser import parse_queue
from LDParser import parse_program
import argparse

def build_script(qFile, pFile):
    queueSpecifier, queueParams = parse_queue(qFile)
    programParams = parse_program(pFile)
    pprint(queueParams)
    pprint(programParams)
    separator = {"PBS":"-", "SBATCH": "--" }
    queueParams["output"] = ""
    queueParams["job-name"] = ""

    #pbs specific settings
    if queueSpecifier == "PBS":
        queueParams["j"] = ["j ", "oe"]
        queueParams["output"] = ["o ", programParams["name"] + ".logs"]
        queueParams["job-name"] = ["N ", programParams["name"]]
    #slurm specific settings
    elif queueSpecifier == "SBATCH":
        queueParams["output"] = ["output=", programParams["name"] + "_%A.out" + ".logs"]
        queueParams["error"] = ["error=", programParams["name"] + "_%A.error" + ".logs"]
        queueParams["job-name"] = ["job-name=", programParams["name"]]

    scriptFile = "./submit-" + programParams["name"] + ".sh"
    sfh = open(scriptFile, 'w')
    sfh.write("#!/bin/bash \n")
    for param in queueParams.keys():
        if param == "launcher" or param == "modules" or (param == "nodes" and queueSpecifier == "PBS"):
            pass
        elif queueParams[param] == "":
            sfh.write("#" + queueSpecifier + " " + separator[queueSpecifier] + param  + "\n")
        else:
            sfh.write("#" + queueSpecifier + " " + separator[queueSpecifier] + queueParams[param][0] + queueParams[param][1] + "\n")
    if queueSpecifier == "PBS":
        sfh.write("#" + queueSpecifier + " " + separator[queueSpecifier] + queueParams["nodes"][0] +
                      queueParams["nodes"][1] + ":ppn=" + queueParams["launcher"]["numProcsPerNode"] + "\n")
    
    # source a local environment if available, makes it easier to give bin files
    sfh.write("if [ -e env.sh ] ; then\nsource env.sh ;\nfi ;\n")
    # source the environment file
    sfh.write("source " + dir_path + "/../env.sh\n")
   
    # call srun/aprun etc.
    cwd = os.getcwd()
    #manipulate modules
    for module in queueParams["modules"]["unload"]:
        sfh.write("module unload " + module + "\n")
    for module in queueParams["modules"]["load"]:
        sfh.write("module load " + module + "\n")

    # srun/aprun launcher line
    sfh.write(queueParams["launcher"]["launch"] + " -n " )
    sfh.write(str(int(queueParams["nodes"][1]) * int(queueParams["launcher"]["numProcsPerNode"])))
    if  queueSpecifier == "SBATCH":
        sfh.write(" -c " +  queueParams["launcher"]["cpuPerTask"])
    sfh.write(" " + dir_path + "/trsched.py --pcfile " + cwd + "/" + pFile)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pcfile", dest = "pcFile", required = True)
    parser.add_argument("--qcfile", dest = "qcFile", required = True)
    args = parser.parse_args()

    build_script(args.qcFile, args.pcFile)

if __name__ == "__main__":
    main()

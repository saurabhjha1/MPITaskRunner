#!/usr/bin/env python

import xml.etree.ElementTree as ET
import re
from pprint import pprint
import sys
import subprocess
import argparse

def get_value(subroutineProragm, routineArgs):
    runCommandName = subroutineProragm  + " "
    for argKey in routineArgs.keys():
        runCommandName += argKey + " " + routineArgs[argKey] + " "
    print(runCommandName)
    p = subprocess.Popen(runCommandName, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    retCode = p.wait()
    if retCode != 0:
        print("Something went wrong while executing subroutine %s" % p.stdout.read())
        exit(1)
    value = re.sub('[\'\n]', '', p.stdout.read())
    return value.split(",")

def parse_program(confFile):
    tree = ET.parse(confFile)
    root = tree.getroot()
    programParams = {}
    for child in root[0]:
        tag = re.sub('[\n\s]', '', child.tag)
        if "executable" in child.tag:
            value = re.sub('[\n]', '', child.text)
        else:
            value = re.sub('[\n\s]', '', child.text)
        attrib = child.attrib
        if   tag == "name":
            programParams["name"] = value
        elif tag == "executable":
            programParams["executable"] = value
        elif tag == "isNamedParameters":
            programParams["isNamedParameters"] = True if value.lower() == "true" else False
        elif tag == "useQueue":
            programParams["useQueue"] = value
        elif tag == "args":
            programParams["args"] = {}
            argNum = 0
            for arg in child:
                argName = arg.attrib["paramName"].strip()
                if "generated" in arg.attrib and arg.attrib["generated"].strip() == "true":
                    subroutineProragm = ""
                    routineArgs = {}
                    for routineChild in arg[0]:
                        routineChildValue = re.sub('[\n\s]', '', routineChild.text)
                        routineChildTag = re.sub('[\n\s]', '', routineChild.tag)
                        if routineChildTag == "name":
                            subroutineProragm = routineChildValue
                        if routineChildTag == "args":
                            for ra in routineChild:
                                routineArgTag = re.sub('[\n\s]', '', ra.tag)
                                routineArgValue = re.sub('[\n\s]', '', ra.text)
                                routineArgAtrrib = re.sub('[\n\s]', '', ra.attrib["paramName"])
                                routineArgs[routineArgAtrrib] = routineArgValue
                    argValue = get_value(subroutineProragm,routineArgs)
                elif "fromFile" in arg.attrib and arg.attrib["fromFile"].strip() == "true":
                    filename = re.sub('[\n\s]', '', arg.text)
                    print("trying to open file %s" % filename)
                    ifh = open(filename, 'r')
                    argValue = re.sub('[\n]','',ifh.readline()).split(",")
                else:
                    argValue = re.sub('[\n\s]', '', arg.text).split(",")
                    print(len(argValue))
                programParams["args"][argNum] = [argName, argValue]
                argNum += 1
    return programParams

def parse_queue(confFile):
    tree = ET.parse(confFile)
    root = tree.getroot()
    queueParams = {}
    queue_specifier = ""
    use_queue = ""
    for child in root:
        tag = child.tag.strip()
        value = child.text.strip()
        attrib = child.attrib
        if tag == "use-queue":
            use_queue = value
        elif tag == use_queue:
            queue_specifier = attrib['name']
            for param in child:
                tag_c = param.tag.strip()
                value_c = param.text.strip()
                if tag_c == "modules":
                    queueParams["modules"] = {"load" : [], "unload" : []}
                    for modtype in param:
                        mod_tag = modtype.tag.strip()
                        mod_val = re.sub('[\n\s]', '', modtype.text).split(",")
                        queueParams["modules"][mod_tag] = mod_val
                    #    pprint(queueParams)
                    #    print(mod_tag, mod_val)
                elif tag_c == "launcher":
                    queueParams["launcher"] = {}
                    for launchParams in param:
                        tag_p = launchParams.tag.strip()
                        value_p = launchParams.text.strip()
                        queueParams["launcher"][tag_p] = value_p
                else:
                    pAttribute = param.attrib["attribute"]
                    queueParams[tag_c] = [pAttribute, value_c]
    return queue_specifier, queueParams


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--qcfile", dest ="qcFile", required = True)
    parser.add_argument("--pcfile", dest = "pcFile", required = True)
    args = parser.parse_args()
    print("Parsed Program File")
    pprint(parse_program(arggs.pcFile))
    print("Parsed Queue File")
    pprint(parse_queue(args.qcFile))

if __name__ == "__main__":
    main()

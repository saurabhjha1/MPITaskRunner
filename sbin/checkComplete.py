#/usr/bin/python
import os
import argparse
from pprint import pprint

def parse_src_list(srcListFile):
    ifh = open(srcListFile, 'r')
    srcFiles = []
    for line in ifh:
        srcFiles.extend(line.split(","))
    srcFiles_ = [f.split("/")[-1] for f in srcFiles]
    return srcFiles_

def get_completed_files(target):
    allFiles = os.listdir(target)
    completedFiles = []
    for f in allFiles:
        if ".complete" in f:
            completedFiles.append(".".join(f.split(".")[0:-1]))
    return completedFiles

def get_target_files(target):
    allFiles_ = os.listdir(target)
    allFiles = []
    for f in allFiles_:
        if os.path.isfile(target + "/" + f):
            allFiles.append(f)
    targetFiles = []
    for f in allFiles:
        if ".complete" not in f:
            targetFiles.append(f)
    return targetFiles

def get_incomplete_files(target, suffix, inputDir, isDel):
    targetFiles = get_target_files(target)
    completedFiles_ = get_completed_files(target)
    completedFiles = [f + suffix for f in completedFiles_]
    incompleteFiles_ = set(targetFiles) - set(completedFiles)
    incompleteFiles = []
    for f in incompleteFiles_:
        incompleteFiles.append(inputDir + "/" + f.replace(suffix, ''))
    for f in incompleteFiles_:
        if isDel:
            os.remove(target + "/" + f)
            print("removed %s" % (target + "/" + f.replace(suffix, '')))
    pprint(incompleteFiles)
    return incompleteFiles
    
def get_missing_files(srcListFile, target, suffix, inputDir):
    srcFiles_ = parse_src_list(srcListFile)
    srcFiles = [ f + suffix  for f in srcFiles_]
    targetFiles = get_target_files(target)
    missingFiles_ = set(srcFiles) - set(targetFiles)
    missingFiles = []
    for f in missingFiles_:
        missingFiles.append(inputDir + "/"  + f.replace(suffix, ''))
    pprint(missingFiles)
    return missingFiles
   
def get_input_dir(srcListFile):
    ifh  = open(srcListFile, 'r')
    inputDir = "/".join(ifh.readline().split(",")[0].split("/")[0:-1])
    ifh.close()
    return inputDir


def write_all(missingFiles, incompleteFiles, outFile):
    ofh  = open(outFile, 'w')
    for f in missingFiles:
        ofh.write(f + ",")
    for f in incompleteFiles:
        ofh.write( f + ",")
    ofh.close()
    return 

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--srclist", required = True, dest = "srcList")
    parser.add_argument("--target", required = True, dest = "target")
    parser.add_argument("--suffix", required = True, dest = "suffix")
    parser.add_argument("--outfile", required = True, dest = "outFile")
    parser.add_argument("--isdel", type=str2bool, default=False, dest = "isDel")
    args = parser.parse_args()
    inputDir = get_input_dir(args.srcList)
    print("getting missing files\n")
    missingFiles = get_missing_files(args.srcList, args.target, args.suffix, inputDir)
    print("getting incomplete files\n") 
    incompleteFiles = get_incomplete_files(args.target, args.suffix, inputDir, args.isDel)
    write_all(missingFiles, incompleteFiles, args.outFile)


if __name__ == "__main__":
    main() 

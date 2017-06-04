#!/usr/bin/env python

import os 
import sys
import argparse

files = []

def bufcount(filename):
    return 1
    if filename == "/projects/HMDR/p0/p0":
        return 0
    f = open(filename)                  
    numlines = 0
    for line in f:
        numlines = numlines + 1
        if numlines > 1:
            return 1
    return 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", dest="src", required=True)
    parser.add_argument("--recursive", dest="recursive", action="store_true", required=False)
    parser.add_argument("--keepwords", dest="keepWords", required = False)
    parser.add_argument("--remwords", dest="remWords", required = False)
    parser.add_argument("--outfile", dest = "outfile", required=True)
    args = parser.parse_args()
    dirs = args.src.split(",")
    isRecEnabled = args.recursive
    keepWords = None
    remWords = None
    if args.keepWords:
        keepWords = args.keepWords.split(",")
    if args.remWords:
        remWords = args.remWords.split(",")
    for src in dirs:
        numFiles = 0
        try:
            for root, dirnames, filenames in os.walk(src):
                for filename in filenames:
                    fileKeepStatus = True # if false file will not be considered
                    fileRemStatus = False # if true file will not be considered
                    if keepWords != None:
                        fileKeepStatus = sum([ (True if f in filename else False) for f in keepWords])
                    if remWords != None:
                        fileRemStatus = sum([(True if f in filename else False) for f in remWords])
                    if fileKeepStatus == True and fileRemStatus == False and bufcount(os.path.join(root, filename)) != 0:
                        files.append(os.path.join(root, filename))
                        numFiles += 1
                if not isRecEnabled:
                    break
        except:
            pass
        #print(src, numFiles)
    print("total files = %d" % len(files))
    ofh = open(args.outfile, 'w')
    ofh.write(",".join(files) + "\n")
    ofh.close()
    print(",".join(files))
if __name__ == "__main__":
    main()

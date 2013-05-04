#!/usr/bin/env python
import os

n1 = int(raw_input())
n2 = int(raw_input())

for i in range(n1, n2):
    str = "receipt-replicate-%d" % i
    os.system("hadoop dfs -copyFromLocal " + "receipt-joined.txt " + " ~/cs525/input/" + str)
    print "copy file:", str

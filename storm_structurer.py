#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
module for unbundle, structuring and save as .csv o .msg stoRM frontend logs

Created on Tue Oct  2 12:33:23 2018

@author: gabriele
"""

import argparse
import pandas as pd
import dateparser

"""
year = '2018/'
in_path = "unstructured_logs/"
out_path = "structured_logs/"
file_name = "storm-frontend-server.log-storm_small"
"""

def log_reader(in_filepath):
    """
    Read stoRM's log file and transform into a list of lines (str)
    
    Recive path (string) to unstructured log file
    Return a list containing where each element is a log's line
    """
    listed_log = []
    
    input_file = open(in_filepath,"r")
    for line in input_file:
        listed_log.append(line.strip())
    input_file.close()
    
    return listed_log

def log_tabler(listed_log):
    """
    Transform a log's list of lines (str) in a dictionary
    
    Recive a list containing where each element is a (stoRM) log's line
    Return a table (dict) where each key is a log's column
    """
    #timestamp is yet to be finished
    date, time_stamp, thread, tipe, token, message = [], [], [], [], [], []
    it = 0
    total = len(listed_log)
    for line in listed_log:
        date.append(line[:18])
        time_stamp.append(dateparser.storm_dtpars(line[:18]))
        thread.append(line.split(" ",4)[3])
        tipe.append(line.split(" ",7)[6])
        token.append(line.split("[",1)[1].split("]",1)[0])
        message.append(line.split(":",3)[3].strip())
        if it%100000 == 0 :
            print " parsed line {0} of {1} lines".format(it,total)
        it+=1
        
    log_table = {'DATE':date, 'TIMESTAMP':time_stamp, 'THREAD':thread,\
                 'TYPE':tipe, 'TOKEN':token, 'MESSAGE':message}
    return log_table

def csver(log_table,out_filepath):
    """
    Transform a log into dictionary form then in .csv
    
    Recive a table (dict) where each key is a log's column
           a string of the filepath output and file name
    Return None
    Produce a structured .csv file of a stoRM log file
    """
    dataf = pd.DataFrame.from_dict(log_table)
    #P: find out columns order
    #print dataf.columns.tolist()
    
    #P: riarrange columns order
    cols =['DATE', 'TIMESTAMP', 'TYPE','THREAD', 'TOKEN','MESSAGE']
    dataf = dataf[cols]
    
    #print dataf.describe()
    dataf.to_csv(out_filepath + '.csv', index=False)



    
csver(log_tabler(log_reader("storm-frontend-server.log-20180901")),"storm-frontend-server.log-20180901")


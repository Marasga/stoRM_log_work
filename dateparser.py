#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 14:53:42 2018

@author: gabriele

function for stoRM frontend logs date manipulation
"""

import datetime

def storm_dtpars(stringa_date):
    """
    Takes stoRM's log datetime string and return timestamp (seconds)
    """
    #NB: ovviamente 2018 è da cambiare nel caso
    formato = '%Y/%m/%d/%H/%M/%S/%f'
    #in alternativa invece dei replace si può semplicemente cambiare formato
    epoch = datetime.datetime(1970,1,1)
    
    date='2018/'+ stringa_date
    date = date.replace(" ","/").replace(":","/").replace(".","/")
    
    date = datetime.datetime.strptime(date,formato)

    time_delta = date - epoch
    timestamp = time_delta.total_seconds()

    return timestamp



# -*- coding: utf-8 -*-
"""
Created on Fri Jul 15 14:44:02 2016

@author: mu529
"""

import numpy as np

def longest_inc_range(s,tolerance=0):
    end_idx_local = 0
    start_idx_local = 0
    best_len = 0
    for i in xrange(1, len(s)):
        if s[i] < (s[i-1]-tolerance):
            # first check we achieved a new best
            local_max_len = end_idx_local - start_idx_local
            if local_max_len > best_len:
                best_len = local_max_len # reset the best length
                start_idx_best = start_idx_local
                end_idx_best = i  # and remember the local indexes as the best 
            # in any case, reset the local bookmarks
            start_idx_local = i
            end_idx_local = i
        else:
            # if it continues to increase, do nothing except move the end bookmark
            end_idx_local = i
            local_max_len = end_idx_local - start_idx_local
    if local_max_len > best_len:
        # in case the entire sequence was monotonic increasing
        return (0,end_idx_local+1)
    elif best_len == 0:
        # in case the entire sequence was monotonic decreasing
        return (None,None)
    else:
        return (start_idx_best,end_idx_best+1)

def align_pwise(a,b,thresh=None):
    if thresh is None:
        thresh = 3*np.std(a+b)/len(a+b)
    res = []
    for i in range(max(len(a),len(b))):
        if abs(b[0] - a[0]) < thresh:
            # within threshold
            res.append((a[0],b[0]))
            a = a[1:]
            b = b[1:]
        else:
            # out-of-threshold
            if a[0] < b[0]:
                res.append((a[0],''))
                a = a[1:]
            elif b[0] < a[0]:
                res.append(('',b[0]))
                b = b[1:]
    return res
    

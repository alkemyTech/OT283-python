#!/usr/bin/env python3
"""reducer.py"""

from operator import itemgetter
from scipy.stats import pearsonr

import numpy as np



import sys

list_num_answ = []
list_num_word = []
current_corr = 0


# input comes from STDIN
for i, line in enumerate(sys.stdin):
    # get values
    len_body, answear_count = line.split('\t')
    try:
        list_num_word.append(int(len_body))
        list_num_answ.append(int(answear_count[:-1]))
    except:
        continue
    if i % 10 == 0 and i != 0:
        # get batch of correlation
        try:
            corr, _ = pearsonr(list_num_word, list_num_answ)
        except:
            continue
        # skip errors
        if np.isnan(corr):
            continue
  

        corr = float(round(corr, 3))
        ## skip initial values
        if current_corr == 0:
            current_corr = corr
        ## que the median of the current corrs
        current_corr = (abs(corr) + abs(current_corr)) / 2
    
        ## erase listts
        list_num_answ = []
        list_num_word = []
    
    # if i >= 10:
    #   print(current_corr)

with open("corr.txt", "w") as f:
    f.write(f'the pearson correlation between the lenght\nof the post body and the number of answears is {current_corr}')
print(f"final correlation {current_corr}")

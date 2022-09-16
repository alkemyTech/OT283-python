#!/usr/bin/env python

from operator import itemgetter
import sys

current_word = {} 

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
    try:
        current_word[word] = current_word[word]+count
    except:
       current_word[word] = count

# do not forget to output the the words and its count
for word in current_word.keys():
    print '%s\t%s' % (word, current_word[word])


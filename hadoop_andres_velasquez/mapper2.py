#!/usr/bin/env python3
"""mapper.py"""

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # ignore irrelevant xml tags
    if 'Body' not in line:
        continue
    # get the body of the post
    items = line.split('"')
    try:
        for i, x in enumerate(items):
            if 'Body' in items[i]:
                body = items[i+1]
                len_body = len(body)
            if 'AnswerCount' in items[i]:
                answear_count = items[i+1]
        
        print(f'{len_body}\t{answear_count}')
    except:
        continue
  
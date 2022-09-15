#!/usr/bin/env python3
"""mapper.py"""

'''
Top 10 de usuarios con mayor porcentaje de respuestas favoritas
'''
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', 
                              '%m-%d-%Y %H:%M:%S')


import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # ignore irrelevant xml tags
    print(line.split('\t'))
    if 'AcceptedAnswerId' not in line:
        continue
    # get the body of the post
    items = line.split('"')
    try:
        for i, x in enumerate(items):
            if 'OwnerUserId' in items[i]:
                user_id = items[i+1]
            if 'FavoriteCount' in items[i]:
                fav_count = items[i+1]
        print(f'{user_id}\t{fav_count}')
    except:
        continue
    #print(f'{user_id =}\t{fav_count =}')





'''
Primero paso por todos los posts y veo el id de respuestas aceptadas
vuelvo a pasar y veo cual es el owner id de las respuestas aceptadas.
Esto lo printeo con owner is , 1
y en el map reduce lo cuento para ver quien es el que tiene mas'''
import xml.etree.ElementTree as ET
from xml.etree import ElementTree
import sys


#REDUCER    
current_item = None
current_count = 0

for item in sys.stdin:
    item = item.strip()
    it, count = item.split()

    try:
        count = int(count)        
    except ValueError:
        continue

    if current_item == it: 
        current_count += count
    else:
        if current_item: #first iteration
            print(current_item, current_count)

        current_item = it
        current_item = count

if current_item == item:
    print(current_item, current_count) #last iteration

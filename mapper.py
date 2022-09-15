import xml.etree.ElementTree as ET
from xml.etree import ElementTree
import sys


#MAPPER
for line in sys.stdin:
 
    line = line.strip()
    words = line.split(' ')
    for word in words:
        items = {word:1}
        print(items)
            #print(f'{word}  ' + 1)

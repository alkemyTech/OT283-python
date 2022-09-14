import xml.etree.ElementTree as ET
from xml.etree import ElementTree
import sys


tree = ET.parse(r"C:\Users\Usuario\Desktop\Escritorio\Univ\Alkemy\Stackoverflow\112010 Meta Stack Overflow\posts.xml")
root = tree.getroot()

#MAPPER
for line in root:
    
    tag = line.tag
    posts = line.attrib
    for line in posts:
        line = line.strip()
        words = line.split(' ')
        for word in words:
            items = {word:1}
            print(items)
             #print(f'{word}  ' + 1)




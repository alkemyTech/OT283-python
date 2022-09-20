

import xml.etree.ElementTree as ET
from xml.etree import ElementTree

'''These functions will allow us to open a xml file and apply a mapreduce to it'''

def open_file():
    with open(f"{PATH}/posts.xml","rt", encoding="utf-8") as file:
        tree=ElementTree.parse(file)
        root = tree.getroot()        
        return tree,root

#MAPPER
def mapper_subtask2(tree,root):
    dictio = {}
    for line in root:    
        tag = line.tag
        posts = line.attrib
        for k, v in posts.items():
            #print(k,v)
            k = k.strip()
            words = k.split(' ')
            for word in words:
                count = int(1)
                dictio.setdefault(word, count)                
                items = (f'{word} {count}')
                #print(items)           
    return dictio


#REDUCER       
def reducer_subtask2(dictio):
    current_item = None
    current_count = 0

    for item in dictio.items():        
        item=str(item)
        item = item.replace('(','')
        item = item.replace(')','')  
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
            #print(it, count)


def main():
    tree, root = open_file()    
    dictio = mapper_subtask2(tree, root)
    reducer_subtask2(dictio)

if __name__=='__main__':
    main()

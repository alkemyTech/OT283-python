import heapq
from multiprocessing import heap
from multiprocessing.heap import Heap
import xml.etree.ElementTree as ET
from xml.etree import ElementTree
from logging import config
from heapq import nlargest
import datetime

PATH = config({'PATH'})

with open(f"{PATH}/posts.xml","rt", encoding="utf-8") as f:
    tree=ElementTree.parse(f)
    print(tree)



### Top 10 answers without accepted answers ###
for node in tree.iter('row'):
    id = node.attrib.get('Id')
    aai = node.attrib.get('AcceptedAnswerId')
    score = node.attrib.get('Score')
    #print(node.tag)

    if not aai:
        if id and score:
            dict = {}
            d = dict.setdefault(id, (int(score)))
            top10 = heapq.nlargest(10, dict, key=dict.get)
            #print(top10)


        #ANOTHER WAY TO GET IT DONE
        #     score_int=int(score)
        #     if score_int >= 270:              

        #         print(f"Id: {id}")
        #         print(f"Score: {score_int}")
        #         #print(' %s' % id)
        #         #print(' %s' % score)
        # else:
        #     print(id)




### Tiempo de actividad de las preguntas 
for node in tree.iter('row'):
    cd = node.attrib.get('CreationDate') #Creation date
    lad = node.attrib.get('LastActivityDate') #Last activity date
    id = node.attrib.get('Id')#Id

    if cd and lad:
        #list creation date
        cd_date= datetime.datetime.strptime(cd,"%Y-%m-%dT%H:%M:%S.%f")
        #list last activity date
        lad_date=datetime.datetime.strptime(lad,"%Y-%m-%dT%H:%M:%S.%f")
        #list the difference between both dates
        active_days= lad_date-cd_date
        
        #print(id,active_days)

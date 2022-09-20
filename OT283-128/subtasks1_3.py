import heapq
from xml.etree import ElementTree
from logging import config
from heapq import nlargest
import datetime


#PATH = config({'PATH'})

#Open the XML file
def open_file():
    with open(f"{PATH}/posts.xml","rt", encoding="utf-8") as file:
        tree=ElementTree.parse(file)
        return tree
    #print(tree)
 
def top_10_naa(tree):#naa: not accepted answers
    ''' This function allows us to filter questions without accepted answers, then it takes the Ids from 
    the 10 most scored questions:
    Firstly we take the indexes we want to take from the XML file, then we select those wichs don't have
    accepted answers, after that we collect them in a dictionary to finally take the top 10 scored Ids 
    question'''
    
    dict = {} #create the dictionary

    for node in tree.iter('row'): #select the indexes we are interested in
        id = node.attrib.get('Id')
        aai = node.attrib.get('AcceptedAnswerId')
        score = node.attrib.get('Score')

        if not aai: #filter questions without accepted answers
            if id and score:
                score_id=int(score)           
                dict.setdefault(id,score_id) # add to dictionary
    top10 = heapq.nlargest(10, dict, key=dict.get) # call top 10 scored questions
    print(top10)
    return(top10)
    

### Tiempo de actividad de las preguntas 
def active_time(tree):
    '''This function allows us to take the activity time from questions by selecting the LastActuvityDate
    less CreationDate then choosing the top10 of questions wichs have been active for more time'''

    dict2 = {} # create a dictionary
    for node in tree.iter('row'): #select indexes we are interested in
        cd = node.attrib.get('CreationDate') 
        lad = node.attrib.get('LastActivityDate') 
        id = node.attrib.get('Id')

        if cd and lad:
            #list creation date
            cd_date= datetime.datetime.strptime(cd,"%Y-%m-%dT%H:%M:%S.%f")
            #list last activity date
            lad_date=datetime.datetime.strptime(lad,"%Y-%m-%dT%H:%M:%S.%f")
            #list the difference between both dates
            active_days= (lad_date-cd_date)
            dict2.setdefault(id, active_days) # add to the dictionary
    top10_adays = heapq.nlargest(10, dict2, key=dict2.get) # call top 10 most active questions
    print(top10_adays)
    return(top10_adays)
#active_time()

def main():
    tree = open_file()
    top_10_naa(tree)
    active_time(tree)

if __name__=='__main__':
    main()

from heapq import nlargest 
import re
from datetime import datetime
import logging
from logging import config
from os import path

#   define log_file_config path
log_file_path = path.join(path.dirname(
    path.abspath(__file__)), "logFileConfig.conf")

#   load logs config form log_file_config
config.fileConfig(log_file_path)
#   define test function

def contarElementosLista(lista):
    """
    Recibe una lista, y devuelve un diccionario con todas las repeticiones de
    cada valor
    """
    return {i:lista.count(i) for i in lista}


def acceptedAnswers(myroot):
    """
        Function select and clean tags atribute and add with aswercount attribute to a list to will be return
        Input:
            xml root
        Output:
            list of tags with accepted aswers
    """
    lista = []
    # to_clean = re.compile('< >')
    # Go along the xml file line by line
    for n in myroot:
    # Go along attributes of xml file
        try:
            cleantext = re.sub(r'\W+', ' ', n.attrib["Tags"])
            cleantext = cleantext.split()
            # Save into lista body tag and answercount, that will be save in a list into list
            # lista.append([n.attrib["Tags"],int(n.attrib["AnswerCount"])])
            lista.append([cleantext,int(n.attrib["AnswerCount"])])
        except:
            continue
    logging.info("ACCEPTED ANSWERS LIST CREATED AND RETURNED")
    # Return list of tags and answerscounts
    return lista   

def wordsRelation(myroot):
    """
        Function that obtain a list of phrases and scores with body attribute and score attribute respectively, finally the function return the list
        Input:
            root xml
        Output:
            list of phrases and score relation
    """
    lista = []
    # Go along the xml file line by line
    for n in myroot:
    # Go along attributes of xml file
        try:
            # Save into lista body attribute and score, that will be save in a list into list
            lista.append([n.attrib["Body"],int(n.attrib["Score"])])
        except:
            continue
    logging.info("WORDS RELATION LIST CREATED A RETURNED")
    # Return list of body(words) and score relations
    return lista

def initialFinalTime(myroot):
    """
        Function that take an xml root and extract CreationDate and CloseDate of posts to append into a list and return it
        Input:
            root xml file
        Output:
            creationdate and closedate list
    """
    lista = []
    # Go along the xml file line by line
    for n in myroot:
    # Go along attributes of xml file
        try:
            # Append into list dates into attributes CreationDate and CloseDate
            lista.append([n.attrib["CreationDate"],n.attrib["ClosedDate"]])
        except:
            continue
    logging.info("INITIAL FINAL TIME DIFERENCE LIST CREATED AND RETURNED")
    # Return list of body(words) and score relations
    return lista

def reducerAcceptedAnswers(list):

    """
        Function that recieve a list and process it to transform that list into a dictionary with sumatory of aswer accepted count that will be return
        Input:
            list with couple tags and asweraccepted count
        Output:
            Dictionary with 10 tags with more accepted aswers
    """
    
    # We create list and dictionary that will we full with tags information
    lista = []
    tagCount = {}

    # Create a list in where load a list of couple tag aswer count acepted
    for items in list:
        for item in items[0]:
            # Walk along items and create a touple with sigle tag and its number of asweraccepted count
            lista.append((item,items[1]))

    for tag in lista:
        # Walk along list to catch tags and put into a dictionary with a sumatory if exist and only added if not exist
        if tag[0] in tagCount:
            tagCount[tag[0]] = tagCount[tag[0]] + tag[1]
        else:
            tagCount[tag[0]] = tag[1]

    # nlargest function reduce dictionary to the indicated number of bigest values, first idicate number ob values, second indicate de dictionary and last indicate the key that we obtain with get
    high = nlargest(10, tagCount, key = tagCount.get)

    logging.info("10 Most Accepted Answers")
    for val in high:
        logging.info(f"{val} : {tagCount.get(val)}")
    
    return high


def reducerWordsRelation(lista):
    """
        Function that take a list with wordsRelation function and transform and count words to then make a relation between length of words and scores of 10 bigest scores into deictionary
        Input:
            root xml
        Output:
            print of relation between length of words and score
    """
    dict = {}
    # Obtain regular expresion to delete xml tags
    to_clean = re.compile('<.*?>')
    for i in lista:
        # Wolk along the list and first clean html tags into body with sub function, then split words to create a list, after that put into dictionary length of words list as a key and score as value
        cleantext = re.sub(to_clean, '', i[0])
        words = cleantext.split()
        dict[len(words)] = i[1]

    # Finally obtain 10 bigest scores in the dictionary to see the relation between length of words and score
    high = nlargest(10, dict, key = dict.get)

    logging.info("Relation between number of words in a post and their score")
    for val in high:
        logging.info(f"{val} : {dict.get(val)}")

    return high

def reducerInitialFinalTime(lista):
    
    """
        Function that take an start and finish date of comments list, then get diference and finally retrun the average of dates
        Input:
            List of inicialdate and finalDate
        Output:
            Rounded average value of time responses
    """

    # We declare list where will be the diference between final time and initial time, and sumatory variable initializee in cero to meke sumatory of dates
    response = []
    sumatory = 0
    
    for i in lista:
        # We walk along the list into list of dates, in eachone we convert string date into especific format date and then make dicerence and extract seconds, finally append it into response list
        response.append((datetime.strptime(i[1],"%Y-%m-%dT%H:%M:%S.%f")-datetime.strptime(i[0],"%Y-%m-%dT%H:%M:%S.%f")).total_seconds())
    for x in response:
        # Walk along response list and meke sumatory of eachone value into a vasriable call sumatory
        sumatory += x
    logging.info("Delay answer in posts")
    # Return average of response times, first make division between sumatory of values and length of response list, then make division with 86400 that are seconds in a day, and in the finish round the value
    logging.info(f"{round((sumatory/len(response))/86400)} days")

    return round((sumatory/len(response))/86400)
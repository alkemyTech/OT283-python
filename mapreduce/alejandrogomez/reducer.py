from functions import reducerWordsRelation, reducerInitialFinalTime,reducerAcceptedAnswers
from mapper import wordList, dateList, answeList
from threading import Thread
import logging
from logging import config
from os import path

#   define log_file_config path
log_file_path = path.join(path.dirname(
    path.abspath(__file__)), "logFileConfig.conf")

#   load logs config form log_file_config
config.fileConfig(log_file_path)
#   define test function


# create two new threads
# We call the function that reduce tag number information in the file
logging.info("REDUCER STARTED")
t1 = Thread(target=reducerAcceptedAnswers(answeList))
t2 = Thread(target=reducerWordsRelation(wordList))
t3 = Thread(target=reducerInitialFinalTime(dateList))
logging.info("REDUCER STOPED")
t1.start()
t2.start()
t3.start()

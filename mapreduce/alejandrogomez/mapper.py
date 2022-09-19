import xml.etree.ElementTree as ET
from pathlib import Path
import os
from functions import acceptedAnswers,wordsRelation, initialFinalTime
import logging
from logging import config
from os import path

#   define log_file_config path
log_file_path = path.join(path.dirname(
    path.abspath(__file__)), "logFileConfig.conf")

#   load logs config form log_file_config
config.fileConfig(log_file_path)
#   define test function

log_file_path = Path(f"{os.path.dirname(os.path.realpath(__file__))}").parent.parent
mytree = ET.parse(f'{log_file_path}/information/posts.xml')
myroot = mytree.getroot()

logging.info("MAPPER STARTED")
# Call function that create a file with tags number information
answeList = acceptedAnswers(myroot)
wordList = wordsRelation(myroot)
dateList = initialFinalTime(myroot)
logging.info("MAPPER STOPED")

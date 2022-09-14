import xml.etree.ElementTree as ET
from pathlib import Path
import os
from functions import acceptedAnswers,wordsRelation, initialFinalTime

log_file_path = Path(f"{os.path.dirname(os.path.realpath(__file__))}").parent
mytree = ET.parse(f'{log_file_path}/information/posts.xml')
myroot = mytree.getroot()

# Call function that create a file with tags number information
answeList = acceptedAnswers(myroot)
wordList = wordsRelation(myroot)
dateList = initialFinalTime(myroot)

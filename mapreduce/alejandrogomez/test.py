from functions import *
from pathlib import Path
import os
import xml.etree.ElementTree as ET

log_file_path = Path(f"{os.path.dirname(os.path.realpath(__file__))}").parent.parent
mytree = ET.parse(f'{log_file_path}/information/posts.xml')
myroot = mytree.getroot()

def test_xmlFile_exist():
    """
        Test if xml file exist and can be import to code
    """
    assert mytree != None

def test_wordsAccepted_mapper():
    """
    tests the wordsAccepted function if it correctly extracts the information from the xml file and if it correctly maps the information into the list
    """
    assert acceptedAnswers(myroot) != None
    assert len(acceptedAnswers(myroot)) > 0

def test_wordsRelation_mapper():
    """
        Tests the wordsRelation function if it correctly extracts the information from the xml file and if it correctly maps the information into the list
    """
    assert wordsRelation(myroot) != None
    assert len(wordsRelation(myroot)) > 0

def test_timeDiference_mapper():
    """
        Tests the timeDiference function if it correctly extracts the information from the xml file and if it correctly maps the information into the list
    """
    assert initialFinalTime(myroot) != None
    assert len(initialFinalTime(myroot)) > 0


def test_wordsAccepted_reducer():
    """
        Tests if the information obtained from the mapper is processed correctly and yields the required 10 values into the reducer wordsAccepted function
    """
    assert reducerAcceptedAnswers(acceptedAnswers(myroot)) != None
    assert len(reducerAcceptedAnswers(acceptedAnswers(myroot))) == 10

def test_wordsRelation_reducer():
    """
        Tests if the information obtained from the mapper is processed correctly and yields the required 10 values into the reducer wordsRelation function
    """
    assert reducerAcceptedAnswers(wordsRelation(myroot)) != None
    assert len(reducerAcceptedAnswers(wordsRelation(myroot))) == 10

def test_timeDiference_reducer():
    """
        Tests if the information obtained from the mapper is processed correctly by calculating the average value
    """
    assert reducerAcceptedAnswers(initialFinalTime(myroot)) != None
    assert len(reducerAcceptedAnswers(initialFinalTime(myroot))) > 0
    
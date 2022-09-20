from xml.dom import NotFoundErr
import pytest
from xml.etree import ElementTree
from subtasks1_3 import active_time, top_10_naa
from mapper_reducer_Subtask2 import mapper_subtask2, reducer_subtask2

'''These functions will allow us to test if the code created for passed tasks works'''

def test_subtasks_1_3():
    '''Function to test the code for task1 and task3'''

    with open(f"{PATH}/posts.xml","rt", encoding="utf-8") as file:
        tree=ElementTree.parse(file)
        #print(tree)

        dict_1 = top_10_naa(tree)
        #print(type(dict_1))
        dict_2 =active_time(tree)
        #print(type(dict_2))

        assert active_time(tree) == ['9508', '7931', '12362', '7046', '11602', '14656', '19470', '19471', '11606', '11610']
        assert top_10_naa(tree) == ['7931', '9138', '9953', '9182', '9135', '9174', '10261', '9235', '9508', '12262']
        # Comprobates if the Ids matches


def test_subtasks_2():
    '''Function to test the code for task2'''
    with open(f"{PATH}/posts.xml","rt", encoding="utf-8") as file:
        tree=ElementTree.parse(file)
        root = tree.getroot()

        mapper = mapper_subtask2(tree, root)
        reducer = reducer_subtask2(mapper)

        assert len(mapper) > 0
        assert len(str(reducer)) > 0 #Convert to str to make it iterable
        #comprobates if mapper and reducer aren't empty


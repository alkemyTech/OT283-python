from functions import reducerWordsRelation, reducerInitialFinalTime,reducerAcceptedAnswers
from mapper import wordList, dateList, answeList
from threading import Thread


# create two new threads
# We call the function that reduce tag number information in the file
t1 = Thread(target=reducerAcceptedAnswers(answeList))
t2 = Thread(target=reducerWordsRelation(wordList))
t3 = Thread(target=reducerInitialFinalTime(dateList))

t1.start()
t2.start()
t3.start()

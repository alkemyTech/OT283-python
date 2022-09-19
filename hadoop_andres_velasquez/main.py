from functions.mapreducers import mapreduce1
from functions.mapreducers import mapper2, mapper3
from functions.mapreducers import reducer2, reducer3

path = '/home/andresv/hadoop_home/hadoop_project/112010 Stack Overflow/posts.xml'

def main():
    ''' Main function to run all the mapreduce functions'''
    mapreduce1(path)
    reducer2(mapper2(path))
    reducer3(mapper3(path))

if __name__ == "__main__":
    main()


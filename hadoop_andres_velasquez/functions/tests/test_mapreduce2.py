from functions import mapreducers

def test_mapreduce2():
    path = '/home/andresport/hadoop_home/hadoop_project/112010 Stack Overflow/posts.xml'
    mapreducers.reducer2(mapreducers.mapper2(path))


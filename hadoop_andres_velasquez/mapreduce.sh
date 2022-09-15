#!/usr/bin/env bash
# change path of the posts.xml file 
# If theres a problem with the hadoop functions uncoment the cat commands
    # to run the map red with only python

# command to erase the output folder each iteration of the command
rm -rf task3.d
rm -rf task2.d
rm -rf output.d
rm -rf corr.txt
rm -rf top_posts_txt
rm -rf top_users_by_favs.txt


# bash script for subtask 1
cat "/home/andresv/hadoop_home/hadoop_project/112010 Stack Overflow/posts.xml" | ./mapper_reducer1.py
# bash script for subtask 2
#cat "/home/andresv/hadoop_home/hadoop_project/112010 Stack Overflow/posts.xml" | ./mapper2.py | ./reducer2.py
mapred streaming -input "/home/andresv/hadoop_home/hadoop_project/112010 Stack Overflow/posts.xml" -output ./task2.d -mapper ./mapper2.py -reducer ./reducer2.py
# hadoop script for substak 3
#cat "/home/andresv/hadoop_home/hadoop_project/112010 Stack Overflow/posts.xml" | ./mapper3.py | ./reducer3.py
mapred streaming -input "/home/andresv/hadoop_home/hadoop_project/112010 Stack Overflow/posts.xml" -output ./task3.d -mapper ./mapper3.py -reducer ./reducer3.py




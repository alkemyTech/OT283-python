from scipy.stats import pearsonr
import os
import numpy as np

path = '/home/andresport/hadoop_home/hadoop_project/112010 Stack Overflow/posts.xml'


def mapreduce1(path):
    """ A function to find the top 10 posts with more accepted answers, it
    both maps and reduces the data in the same function.
    Args:
        -path (str): path to the posts.xml file in the stack overflow dataset
    """
    with open(path, 'r') as f:
        top_posts = []
        count = 0
        for line in f:
            if 'AnswerCount' not in line:
                continue
            # get the body of the post
            count += 1
            print(f'analizing line {count}')
            items = line.split('"')
            for i, x in enumerate(items):
                if 'AnswerCount' in items[i]:
                    try:
                        num_answers = int(items[i+1])
                    except:
                        continue
                if 'Id' in items[i]:
                    try:
                        id_post = int(items[i+1])
                    except:
                        continue
            ## analisis
            if len(top_posts) < 10:
                top_posts.append([id_post, num_answers])
            ## sort
            top_posts = sorted(top_posts, key=lambda x: x[1])

            if num_answers > top_posts[0][1]:
                top_posts.pop(0)
                top_posts.append([id_post, num_answers])

        with open('top_posts.txt', 'w') as f:
            for i, x in enumerate(top_posts):
                print(f'#{10-i} post_id: {x[0]}, number of answers: {x[1]}')
                f.write(f'#{10-i} post_id: {x[0]}, number of answers: {x[1]}\n')

def mapper2(path):
    """ A mapper function that prints to stdout the lenght of the body of every question
    type post in the stack overflow dataset, and the number of answers that post has.
    This function is used with the reducer2 function to get the correlation index
    of the number of words in the body of a question and its number of answers.
    Args:
        path (str): path to the posts.xml file in the stack overflow dataset
    """
    with open(path, 'r') as f:
        for line in f:
            # ignore irrelevant xml tags
            if 'Body' not in line:
                continue
            # get the body of the post
            items = line.split('"')
            try:
                for i, x in enumerate(items):
                    if 'Body' in items[i]:
                        body = items[i+1]
                        len_body = len(body)
                    if 'AnswerCount' in items[i]:
                        answear_count = items[i+1]
                
                yield (f'{len_body}\t{answear_count}')
            except:
                continue

def reducer2(f):
    """ A reducer function that depends on the mapper2 function that calculates the 
    pearson correlation index for every 10 posts and average the score. At the end of the
    execution it writes to a file final correlation number.
    type post in the stack overflow dataset, and the number of answers that post has.
    Args:
        path (str): path to the posts.xml file in the stack overflow dataset
    """
    for line in f:
        list_num_answ = []
        list_num_word = []
        current_corr = 0
        # input comes from STDIN
        for i, line in enumerate(f):
            # get values
            len_body, answear_count = line.split('\t')
            try:
                list_num_word.append(int(len_body))
                list_num_answ.append(int(answear_count[:-1]))
            except:
                continue
            if i % 10 == 0 and i != 0:
                # get batch of correlation
                try:
                    corr, _ = pearsonr(list_num_word, list_num_answ)
                except:
                    continue
                # skip errors
                if np.isnan(corr):
                    continue
        

                corr = float(round(corr, 3))
                ## skip initial values
                if current_corr == 0:
                    current_corr = corr
                ## que the median of the current corrs
                current_corr = (abs(corr) + abs(current_corr)) / 2
            
                ## erase listts
                list_num_answ = []
                list_num_word = []
            
            # if i >= 10:
            #   print(current_corr)
            print(f"final correlation {current_corr}")
            with open("corr.txt", "w") as t:
                t.write(f'the pearson correlation between the lenght\nof the post body and the number of answears is {current_corr}')

# def mapreduce2(path):
#      """ A mapper function that prints to stdout the lenght of the body of every question
#     type post in the stack overflow dataset, and the number of answers that post has.
#     This function is used with the reducer2 function to get the correlation index
#     of the number of words in the body of a question and its number of answers.
#     Args:
#         path (str): path to the posts.xml file in the stack overflow dataset
#     """
#     os.system(f"python3 mapreducers.py mapper ")

def mapper3(path):
    """ This is the mapper function to calculate the top users with more
    favororite counts in the stack overflow dataset. 
    It gets the OwnerUserId and the FavoriteCount of every post and prints it.
    It is used with the reducer3 function to answear the subtask number 3.
        path (str): path to the posts.xml file in the stack overflow dataset
    """ 
    with open(path, 'r') as f:
    # input comes from STDIN (standard input)
        for line in f:
            # ignore irrelevant xml tags
            print(line.split('\t'))
            if 'AcceptedAnswerId' not in line:
                continue
            # get the body of the post
            items = line.split('"')
            try:
                for i, x in enumerate(items):
                    if 'OwnerUserId' in items[i]:
                        user_id = items[i+1]
                    if 'FavoriteCount' in items[i]:
                        fav_count = items[i+1]
                print(f'{user_id}\t{fav_count}')
            except:
                continue

def reducer3(f):
    """ This is the reducer function to calculate the top users with more
    favororite counts in the stack overflow dataset. 
    It creates a changing top 10 list with the users with more favorite counts.
    This list gets order each passing of the loop and checks if the new user
    has more favorite counts than the last one in the list, and thus pops the
    lesser value and appends the new one.
    It is used with the mapper3 function to answear the subtask number 3.
        path (str): path to the posts.xml file in the stack overflow dataset
    """ 
    for line in f:
        user, fav_count  = line.split('\t')
        user = user.strip()
        fav_count = (fav_count.strip())
        ###print(f'{user}|{fav_count}')


        if len(top_users) >= 10:
            top_users = sorted(top_users, key=lambda x: x[1])
            if fav_count > top_users[0][1]:
                top_users.pop(0)
                top_users.insert(0,[user, fav_count])
        else:
            top_users.append([user, fav_count])

        print(top_users)

    top_users = sorted(top_users, key=lambda x: x[1])
    with open('top_users_by_favs.txt', 'w') as f:
        for i, x in enumerate(top_users):
            print(f'#{10-i} post_id: {x[0]}, number of answers: {x[1]}')
            f.write(f'#{10-i} post_id: {x[0]}, number of answers: {x[1]}\n')
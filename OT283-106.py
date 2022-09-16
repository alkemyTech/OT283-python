import numpy as np
import pandas as pd
import xml.etree.ElementTree as et
from lxml import etree
from datetime import date
from operator import itemgetter
import operator

# Create a function to read the xml and convert each line to dictionary and avoid consuming
# all RAM and Caché memory and at the end return a Pandas DataFrame
def read_xml(filepath,tag,*args):
    """
    I built this function is an optimized version to parse large XML files with different
    size of attributes for each line of tags

    args:
    filepath : path of xml file
    tag : tag name of each line
    *args : attributes that want to be turn into columns 
    """
    atts=[arg for arg in args]
    dict_list = []
    
    for _, elem in etree.iterparse(filepath, events=("end",),recover=True):

        tempdict={}
        if elem.tag == tag:
            line_att=np.array([i for i in elem.attrib])
            line_att=line_att[np.isin(line_att,atts)]
            f = operator.itemgetter(*line_att)
            tempdict=dict(zip(line_att,f(elem.attrib)))

            dict_list.append(tempdict)
            elem.clear()

    return pd.DataFrame(dict_list,dtype=object).fillna(np.nan)

# Run the function to read xml files selection only the attributes on interest
stack_post= read_xml(r"112010 Stack Overflow\posts.xml",'row','Id','PostTypeId','ParentId','CreationDate','Score')

# Reformat CreationDate colummn with datetime to D-M-Y 
stack_post['CreationDate']=pd.to_datetime(stack_post['CreationDate'],format="%Y-%m-%dT%H:%M:%S.%f")


print ('The top 10 of dates with lowest ammount of post are \n ',stack_post.groupby(['CreationDate'])['Id'].count().sort_values(ascending=True)[:10])

# 
# To solve question #3 :
# Rank 100 to 200 top post by Score and new column about average time of answer 

# First the Score column must be changed to int data type

stack_post['Score']=stack_post.Score.astype(int)

# To define the Top ranked post I considered a good post as the equivalence of suming all answers in the post 
# therefore it requires grouping by ParentId 

stack_post.ParentId.fillna(stack_post.Id,inplace=True) # Question does not have ParentId therefore it is filled with Post Id

Top200_SumScore = stack_post.groupby(by='ParentId')['Score'].sum().sort_values(ascending=False) # So we got top 200 best scored Post 
 
# Now we need to add average time of answer

pivot_postavg=stack_post.pivot_table(index='ParentId',values='CreationDate',columns='PostTypeId',aggfunc=np.mean)

pivot_postavg['Delta Avg']=pivot_postavg.loc[:,'2']-pivot_postavg.loc[:,'1']

# then we set the column values for each post Id in Top200_SumScore

Top200_SumScore['Delta Avg']=pivot_postavg.loc[Top200_SumScore.ParentId,:]['Delta Avg']

# for curios metrics we estime the minimum time between answer of each post

pivot_postmin=stack_post.pivot_table(index='ParentId',values='CreationDate',columns='PostTypeId',aggfunc=min)

pivot_postmin['Delta Min']=pivot_postmin.loc[:,'2']-pivot_postmin.loc[:,'1']

# then we set the column values for each post Id in Top200_SumScore

Top200_SumScore['Delta Min']=pivot_postmin.loc[Top200_SumScore.ParentId,:]['Delta Min']


# To solve question # 2:
# Top 10 most stated words on the posts

# First we need to have the text of every post in a single txt file 

stack_post_body= read_xml(r"112010 Stack Overflow\posts.xml",'row','Id','Body')
stack_post_body['Body'].to_csv('Post_body.txt',encoding='utf-8',index=False,header=False)

#it results on a txt file that each line contain every post content

# next functions mapper.py and reducer.py are executed over the file Post_body.txt
# and results are saved on wordspost.txt

# To get the top 10 most stated words the resulting file is readed as a DataFrame

word_count_post = pd.DataFrame([line.rstrip().split('\t') for line in open('wordspost.txt')],columns=['word','count'])

word_count_post['count']= word_count_post['count'].astype('int')

# This is the result of most stated words, however most of them are not KEYWORDS
# These words are secondary words that do not add value to the task
result_1=word_count_post.sort_values(by='count',ascending=False)[:11]

# Result 2 would allow us to define more representative KEYWORDS.
result_2=word_count_post.sort_values(by='count',ascending=False)[:20]

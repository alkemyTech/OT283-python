import logging, os
import pandas as pd
from sqlalchemy import exc, create_engine
from decouple import config
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3


def extract(files_folders, engine, query_filename):
    '''
    Function to extract data from the database to a csv file
    '''
    logging.info(('Extracting data from{}'.format(query_filename)))
    #Read the query from the file and execute it
    with engine.connect() as con:
        with open(f"{files_folders['sql']}{query_filename}") as file:
            query = file.read()
            logging.info(('query to be executed{}'.format(query)))
            # Erasing the .sql of the file name
            filename = query_filename[:-4]
            logging.info(f'Saving data in filename: {filename}')
            logging.info('Executing query')               
            result_q = con.execute(query)
            logging.info('Query executed')
            # Saving the result in a csv file
            df = pd.DataFrame(result_q)
            os.makedirs('files',exist_ok=True)
            df.to_csv(f'files/{filename}.csv', index=False)
            logging.info('succesfully created the csv files')

    return 0
                

def transform(query_filename, files_folders):
    '''
    this function check which query is it and then transform the data accordingly'''
    logging.info('Transforming data')
    ''' Utility functions for the upcoming transform functions '''       
    # replacing '/' for '-' in inscription date.
    def changeWord(word,og_char,new_char):
        '''
        Replace an old character (og_char) in a string 
        with a new character (new_char).
        '''
        for letter in word:
            if letter == og_char:
                word = word.replace(letter,new_char)
        return word

    #Recursive function to erase blank spaces in a columns
    def erase_character(word, char):
        '''
        Erases the character (char) in the word (word)
        if it is found at the start or at the end of the word
        '''
        if word[-1] == char:
            return erase_character(word[:-1], char)
        if word[0] == char:
            return erase_character(word[1:], char)
        else:
            return word
    
    query_filename= query_filename[:-4]
    if query_filename == 'query_jujuy':
        #transform the extracted data
        logging.info('transforming data of {query_filename}.csv')
        #Load data from the csv. Change this path in production
        df = pd.read_csv('{}{}.csv'.format(files_folders['main'],query_filename))
        df.columns = [
            'age', ## take out the word days to leave the bare number of days
            'first_name', #ok
            'last_name',  #ok
            'postal_code', #check it from the repo
            'university', #ok
            'career', # quitar espacios adicionales. Hacer una funcion para todas
            'inscription_date', # ok. pero preguntar si hay que cambiarla
            'gender', # cambiar a malo or female
            'location', # ver si esta es la que es
            'email',  # ok
        ]
        #Change order of the columns
        df = df[['university','career','inscription_date','first_name','last_name','gender','age']]
        ## cleaning age column
        df.age = df.age.apply(lambda x: x[:-5])
        
        df.career = df.career.apply(lambda x: erase_character(x, ' '))
        #Change gender values to male or female
        #check for invalid character in gender column
        if len(df.loc[(df.gender != 'm') & (df.gender != 'f')]) != 0:
            print('Theres invalid characters in the gender column\n the next cleaning operation wont work')
        df.gender = df.gender.apply(lambda x: 'male' if x == 'm' else 'female')

    
        df.inscription_date = df.inscription_date.apply(lambda x: changeWord(x,'/', '-'))
        #Add the data frame to a text file
        logging.info('saving data to universidad_jujuy_cleaned.txt')
        df.to_csv('{}{}_cleaned.txt'.format(files_folders['main'], query_filename[:-4]), sep=',', index=False)

    elif query_filename == 'query_palermo':
    
        ############## Clean the palermo data ###################
        logging.info('transforming data of query_palermo.csv')
        #Load data from the csv. Change this path in production
        dfp = pd.read_csv('{}{}.csv'.format(files_folders['main'], query_filename))
        dfp.columns = [
            'age', # make inte and take out '_'s
            'first_name',
            'last_name',
            'postal_code', #load the value from the repo
            'university', #replace '_' with ' ' make lowercase and take out edge spaces
            'career', 
            'inscription_date', 
            'gender', 
            'location', 
            'email'
        ]
        #Change order of the columns
        dfp = dfp[['university','career','inscription_date','first_name','last_name','gender', 'age', 'postal_code','location', 'email']]
        #Remove word days from age column
        dfp.age = dfp.age.apply(lambda x: int(x[:-5]))
        #clean univerity column
        dfp.university = dfp.university.apply(lambda x: changeWord(x,'_',' ').lower())
        dfp.university = dfp.university.apply(lambda x: erase_character(word=x,char=' '))
        #Clean career column
        dfp.career = dfp.career.apply(lambda x: changeWord(x,'_',' ').lower())
        dfp.career = dfp.career.apply(lambda x: erase_character(word=x,char=' '))
        #Clean gender column
        if len(dfp.loc[(dfp.gender != 'm') & (dfp.gender != 'f')]) != 0:
            print('Theres invalid characters in the gender column\n the next cleaning operation wont work')
        dfp.gender = dfp.gender.apply(lambda x: 'male' if x == 'm' else 'female')

        #Send to .txt file
        logging.info('saving data to {}{}_cleaned.txt'.format(files_folders['main'], query_filename))
        dfp.to_csv('{}{}_cleaned.txt'.format(files_folders['main'], query_filename), sep=',', index=False)

    else:
        logging.info('No cleaning protocol found for {} query'.format(query_filename))
        return 1
    



def load_data(filename, key, bucket_name):
    '''''
    Function to upload files into a S3 Bucket
    * File name -> File path
    * Key -> file name with extension
    * bucket_name -> bucket name configurated in s3 service 
    '''
    logging.info('loading data to S3')
    logging.info(f'{filename=}, {key=}, {bucket_name=}')
    hook = S3Hook('s3_mine')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name,acl_policy='public-read') 



def upload():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('pipelonchos6969')
    bucket.upload_file(Filename='/home/andresv/coding/airflow2/files/universidad_jujuy_cleaned.txt', Key='universidad_jujuy_cleaned.txt')
    print(bucket)



if __name__ == '__main__':
    #     # loading the env variables 
    # db_host = config('DB_HOST')
    # db_port = config('DB_PORT')
    # db_user = config('DB_USER')
    # db_pass = config('DB_PASSWORD')
    # db_name = config('DB_NAME')

    # # Creating the db engine
    # engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user,db_pass,db_host,db_port,db_name))
    # extract(engine=engine,
    #  sql_filename1='query_jujuy.sql',
    #  sql_filename2='query_palermo.sql')
    # load_data(
    #     filename='/files/universidad_palermo_cleaned.txt',
    #     key='universidad_palermo_cleaned.txt',
    #     bucket_name= 'cohorte-agosto-38d749a7'
    pass


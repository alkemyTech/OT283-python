import logging, os
import pandas as pd
from sqlalchemy import exc, create_engine
from decouple import config


def extract(**kwargs):
    ### getting the name of the sql files
    lista_queries = [v for k, v in kwargs.items() if type(v) == str and 'sql_filename' in k]
    if not lista_queries:
        logging.info("Theres no queries to process. skipping task")
        return 0
    #logging.info(('list with the names of sql files{}'.format(lista_queries)))

    engine = kwargs['engine']
    ### For Loop to extract data from all the query names injected in the dag op_args parameter
    for query_file in lista_queries:
        with engine.connect() as con:
            with open(f"dags/sql/{query_file}") as file:
                query = file.read()
                #logging.info(('query to be executed{}'.format(query)))
                # Erasing the .sql of the file name
                filename = query_file[:-4]
                print(f'Saving data in filename: {filename}')
                print('Executing query')               
                result_q = con.execute(query)
                #logging.info('Adding query results to a dataframe')
                df = pd.DataFrame(result_q)
                os.makedirs('files',exist_ok=True)
                df.to_csv(f'files/{filename}.csv', index=False)
                #logging.info('succesfully created the csv files')
                
                

def transform():
    #transform the extracted data
    logging.info('transforming data of query_jujuy.csv')
    #Load data from the csv. Change this path in production
    df = pd.read_csv('files/query_jujuy.csv')
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
    df.career = df.career.apply(lambda x: erase_character(x, ' '))
    #Change gender values to male or female
    #check for invalid character in gender column
    if len(df.loc[(df.gender != 'm') & (df.gender != 'f')]) != 0:
        print('Theres invalid characters in the gender column\n the next cleaning operation wont work')
    df.gender = df.gender.apply(lambda x: 'male' if x == 'm' else 'female')

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
    df.inscription_date = df.inscription_date.apply(lambda x: changeWord(x,'/', '-'))
    #Add the data frame to a text file
    logging.info('saving data to universidad_jujuy_cleaned.txt')
    df.to_csv('files/universidad_jujuy_cleaned.txt', sep=',', index=False)
    
    ############## Clean the palermo data ###################
    logging.info('transforming data of query_palermo.csv')
    #Load data from the csv. Change this path in production
    dfp = pd.read_csv('files/query_palermo.csv')
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
    if len(dfp.loc[(df.gender != 'm') & (dfp.gender != 'f')]) != 0:
        print('Theres invalid characters in the gender column\n the next cleaning operation wont work')
    dfp.gender = dfp.gender.apply(lambda x: 'male' if x == 'm' else 'female')

    #Send to .txt file
    logging.info('saving data to files/universidad_palermo_cleaned.txt')
    dfp.to_csv('files/universidad_palermo_cleaned.txt', sep=',', index=False)
    



def load():
    #load data 
    logging.info('loading data')


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
    transform()


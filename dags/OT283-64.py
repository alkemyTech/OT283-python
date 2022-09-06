import time
from sqlalchemy import exc, create_engine
from datetime import datetime, timedelta
import logging
import psycopg2
import pandas as pd
import os
import numpy as np

    #DB_connection
def extract():

    #connect the database            
    engine = create_engine("postgresql://{}:{}@{}:{}/{}")  #postgresql://user:password@host:port/db" 
    engine.connect()
  
    #Read sql files
    SQL_DIR = os.path.abspath(os.path.dirname(__file__))
    sql1_ub = os.path.join(SQL_DIR, "universidad_nacional_tres_de_febrero.sql")
    with open(sql1_ub) as  universidad1:
        unf= universidad1.read()                             
    #apply the sql query to extracted data from postgresql
    unf_filtered = pd.read_sql(unf,engine)

    #repeat the process for the second query
    sql2_ub = os.path.join(SQL_DIR, "universidad_tecnologica_nacional.sql")
    with open(sql2_ub, encoding='UTF-8') as universidad2:
        utn= universidad2.read()
    utn_filtered = pd.read_sql(utn,engine) 

    #return db filtered
    return unf_filtered, utn_filtered  

    
def norm(univ1, univ2):
    ### NORMALIZATION PROCESS FOR BOTH UNIVERSITIES
    #University, career, first_name, last_name, location and email normalization
        #create functions      
    series_str = lambda x: str(x) #function to convert to str
    series_int = lambda x: int(x) #function to convert to int
    series_lower = lambda x: x.lower() #function to convert to lower
    series_replace = lambda x: x.replace('_', ' ') #function to delete dashes
    series_replace_m = lambda x: x.replace('m', 'male')#function to replace m to male
    series_replace_f = lambda x: x.replace('f', 'female')#function to replace m to female
    series_strip = lambda x: x.strip() #delete extra spaces

    #UNIVERSIDAD NACIONAL TRES DE FEBRERO NORMALIZATION
    #ucflle: is an acronym of university, career, first_name, last_name, location and email
    ucflle_str1 = univ1[['university','career','first_name', 'last_name','location','email']].applymap(series_str)#convert to str
    ucflle_lower1 = ucflle_str1.applymap(series_lower)#convert to lower
    ucflle_replace1 = ucflle_lower1.applymap(series_replace)#delete dashes
    ucflle1 = ucflle_replace1.applymap(series_strip) #delete extra spaces

    #inscription_date normalization
    insc_date1=univ1[['inscription_date']].applymap(series_str)

    #postal_code normalization
    pcod_str1= univ1[['postal_code']].applymap(series_str)

    #age normalization
    age_int1= univ1[['age']].applymap(series_int)

    #gender normalization
    m_replace1 = univ1[['gender']].applymap(series_replace_m)
    gen_replace1= m_replace1.applymap(series_replace_f)

    #concatenate dataframes    
    university1=pd.concat([ucflle1,pcod_str1,age_int1,insc_date1,gen_replace1],axis=1)
    #reorder dataframe
    university1=university1[['university','career','inscription_date','first_name','last_name','gender','age','postal_code','location','email']]
    print(university1)
    print(university1.dtypes)


    #UNIVERSIDAD TECNOLÓGICA NACIONAL NORMALIZATION
    #repeat the process 
    #ucflle: is an acronym of university, career, first_name, last_name, location and email
    ucflle_str2 = univ2[['university','career','first_name', 'last_name','location','email']].applymap(series_str)#convert to str
    ucflle_lower2 = ucflle_str2.applymap(series_lower)#convert to lower
    ucflle_replace2 = ucflle_lower2.applymap(series_replace)#delete dashes
    ucflle2 = ucflle_replace2.applymap(series_strip) #delete extra spaces

    #inscription_date normalization
    insc_date2=univ2[['inscription_date']].applymap(series_str)

    #postal_code normalization
    pcod_str2= univ2[['postal_code']].applymap(series_str)

    #age normalization
    age_int2= univ2[['age']].applymap(series_int)

    #gender normalization
    m_replace2 = univ2[['gender']].applymap(series_replace_m)
    gen_replace2= m_replace2.applymap(series_replace_f)

    #concatenate dataframes    
    university2=pd.concat([ucflle2,pcod_str2,age_int2,insc_date2,gen_replace2],axis=1)
    #reorder dataframe
    university2=university2[['university','career','inscription_date','first_name','last_name','gender','age','postal_code','location','email']]
    print(university2)
    print(university2.dtypes)

    #create txt directory and .txt files
    os.makedirs('txt_files', exist_ok=True)
    university1.to_csv('txt_files/universidad_nacional_tres_de_febrero.txt',index=False, sep=';')
    university2.to_csv('txt_files/universidad_tecnologica_nacional.txt', index=False, sep=';')
 
def main():
    univ1,univ2=extract()
    norm(univ1,univ2)

if __name__ == '__main__':
    main()      

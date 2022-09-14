# import required modules
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from mysql.connector import connect, Error

# function to obtain url and parse data 
def get_page(offset):
    url1 = f'https://www.freshersworld.com/jobs/jobsearch/data-analyst-jobs-in-bangalore?&limit=30&offset={offset}'
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"}

    response = requests.get(url1, headers=headers)
    if(response.status_code != 200):
        print("Response code: ", response.status_code)
        raise Exception(f"Couldn't fetch {url1}")

    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

#empty lists to store the extracted values in 
company_list = []
role_list = []
quals_list = []
ld_list = []
exp_list = []

# function to extract the data
def extract_data(soup):
    
    divs = soup.find_all('div', class_='col-md-12 col-lg-12 col-xs-12 padding-none job-container jobs-on-hover top_space')
    for item in divs:
        company = item.find('h3', class_='latest-jobs-title font-16 margin-none inline-block').text.strip()
        company_list.append(company)

        role = item.find('div', class_ = 'col-md-12 col-xs-12 col-lg-12 padding-none left_move_up').find('div').text.strip()
        role_list.append(role)

        quals = item.find('span', class_='bold_elig').text.strip()
        quals_list.append(quals)

        last_date = item.find('span', class_='padding-left-4').text.strip()
        ld_list.append(last_date)

        exp = item.find('span', class_='experience').text.strip()
        exp_list.append(exp)

        
        # dict to be passed into a list
        job_dict = {
            'Company' : company,
            'Role': role,
            'Qualification': quals,
            'Last Date to Apply': last_date,
            'Experience Required': exp
        }
        
        joblist.append(job_dict)

    return 

joblist = []

#steps of 30 as the site has only 3 pages and page value jumps by 30
for i in range(0, 90, 30):
    soup = get_page(i)
    extract_data(soup)

df = pd.DataFrame(joblist)
# print(df.head())
df.to_csv('Job_List.csv', index=None)

#uploading to a database

try:
    with connect(
        host = 'localhost',
        user = os.environ.get('SQL_USER'),
        passwd = os.environ.get('SQL_PASS'),
        database = 'fresher_world'
    ) as connection:

        # creating the database
        #create_db_query = '''CREATE DATABASE fresher_world'''
        #with connection.cursor() as cursor:
        #    cursor.execute(create_db_query)

        #query to create the table
        create_table_query = '''CREATE TABLE fworld_data(
            company VARCHAR(100),
            role VARCHAR(100),
            qualification VARCHAR(100),
            last_date VARCHAR(100),
            experience VARCHAR(100)
        )'''


        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            
        #inserting the records into the created table
            for index, row in df.iterrows():
                company = company_list[index]
                role = role_list[index]
                qualification = quals_list[index]
                last_date = ld_list[index]
                experience = exp_list[index]

                query = "INSERT INTO fworld_data VALUES(" "\"" + str(company) + "\"" + "," + "\"" + str(role) + "\"" + "," + "\"" + str(qualification) + "\"" + "," + "\"" + str(last_date) + "\"" + "," + "\"" + str(experience) + "\"" + ")"
                
                with connection.cursor() as cursor:
                    #print(query)
                    cursor.execute(query)
            
        connection.commit()
            
except Error as e:
    print(e)
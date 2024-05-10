# import necessary libraries
import requests
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import json
from datetime import datetime
import numpy as np
import os

###### Define functions ######
# Develop a function to retrieve data from the platform via API call
def get_ticket_data(url):
    headers = {
        "User-Agent": "Python TeamSupport API Client",
        "Accept": "application/json",
        "Authorization": f"Basic {org_id}",
    }
    try:
        response = requests.get(url, headers=headers, auth=(org_id, access_token))
        response.raise_for_status()
        ticket_data = response.json()
        return ticket_data["Tickets"]
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return None

# Develop a function to capture the specific requested info (reports) from the platform
def ticket_insert_prep(ticket_df):
    # seperate 2 types of tickets, some fields only exist in Dep A
    if len(ticket_df[ticket_df.TicketTypeName=='DepartmentA'])>0:
        DepartmentA_Tickets=ticket_df[ticket_df.TicketTypeName=='DepartmentA'][["ID","TicketTypeName",'UserName',"TicketNumber","Name","DateCreated","IsClosed","DateClosed","GroupName","ProductName","Severity","Status","PrimaryCategory","PrimaryCustomer"]]
        #rename some fields for consistency
        DepartmentA_Tickets.columns=["ID","TicketTypeName",'UserName',"TicketNumber","Name","DateCreated","IsClosed","DateClosed","GroupName","ProductName","Severity","Status",'primarycategory_departA',"PrimaryCustomer"]
    else:
        ALL_Tickets = pd.DataFrame(columns=["ID","TicketTypeName",'UserName',"TicketNumber","Name","DateCreated","IsClosed","DateClosed","GroupName","ProductName","Severity","Status",'primarycategory_departA',"PrimaryCustomer"])
    
    if len(ticket_df[ticket_df.TicketTypeName=='DepartmentB'])>0:
        DepartmentB_Tickets=ticket_df[ticket_df.TicketTypeName=='DepartmentB'][["ID","TicketTypeName",'UserName',"TicketNumber","Name","DateCreated","IsClosed","DateClosed","GroupName","ProductName","Severity","Status","Initiation","RootCause","PrimaryCustomer",'PrimaryCategory']]
        #rename some fields for consistency
        DepartmentB_Tickets.columns = ["ID","TicketTypeName",'UserName',"TicketNumber","Name","DateCreated","IsClosed","DateClosed","GroupName","ProductName","Severity","Status","Initiation","RootCause","PrimaryCustomer",'primarycategory_departB']
    else:
        DepartmentB_Tickets= pd.DataFrame(columns=['ID','TicketTypeName','UserName','TicketNumber','Name','DateCreated','IsClosed','DateClosed','GroupName','ProductName','Severity','Status','Initiation','RootCause','PrimaryCustomer','primarycategory_departB'])
    #concat info from 2 different groups
    overview=pd.concat([DepartmentB_Tickets,ALL_Tickets],axis=0,sort=False).reset_index(drop=True)
    # convert the column to datetime type
    overview['DateCreated'] = pd.to_datetime(overview['DateCreated'])
    overview['DateClosed'] = pd.to_datetime(overview['DateClosed'])
    overview=overview[['ID','TicketTypeName','UserName','TicketNumber','Name','DateCreated','IsClosed','DateClosed','GroupName','ProductName','Severity','Status','Initiation','RootCause','PrimaryCustomer','primarycategory_departA','primarycategory_departB']]
    return overview

# Develop a function to load data in batches
def batch_load(inserted_table,df):
    # define the batch size
    batch_size = 1000
    # iterate over the DataFrame in batches and insert data to Database
    for i in range(0, len(df), batch_size):
        batch = df[i:i + batch_size]
        batch.to_sql(inserted_table, engine, index=False, if_exists='append',schema="temp")
        print("batch{}Insertion completed.".format(i))
    
# Develop a function to load data into temp table
def batch_load_temp_tuple(tuple_list):   
    print('total insert to temp ',len(tuple_list))
    row_num=0
    with conn.cursor() as cursor:
        # Iterate over the DataFrame in batches and insert data to database
        for i in tuple_list:
            row_num=row_num+1
            insert_query = "insert into temp_api_data(ID ,TicketTypeName,UserName,TicketNumber,Name,DateCreated,IsClosed,DateClosed,GroupName,ProductName,Severity,Status,Initiation,RootCause,PrimaryCustomer,primarycategory_departA,primarycategory_departB) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(insert_query,i)            
        conn.commit()

# Develop a function to detect updates in the platform for entries already logged in the table and update the corresponding information in the table.
def update_ticket_tb(url):
    # extract data from API call
    url_tickets_df = ticket_insert_prep(pd.DataFrame(get_ticket_data(url)))
    # Transform NaT to None
    df_transformed = url_tickets_df.replace({np.NaN: None})
    url_tickets_tuples=list(df_transformed.itertuples(index=False, name=None))
    
    # create a temp table to hold old tickets extracted from API call
    with conn.cursor() as cursor:
        TEMP = "DROP TABLE IF EXISTS temp_api_data; CREATE TEMP TABLE temp_api_data(ID varchar,TicketTypeName varchar,UserName varchar,TicketNumber varchar,Name  varchar,DateCreated timestamp,IsClosed  varchar,DateClosed timestamp null,GroupName  varchar,ProductName  varchar,Severity varchar,Status varchar,Initiation varchar,RootCause varchar,PrimaryCustomer varchar,primarycategory_departA varchar,primarycategory_departB varchar);"
        cursor.execute(TEMP)
        conn.commit()
    batch_load_temp_tuple(url_tickets_tuples)

    # identify tickets that have info requires update in the downstream table
    with conn.cursor() as cursor:
        FOR_UPDATED = "Select * FROM schema.ticketTable a JOIN temp_api_data b ON a.id = b.id WHERE coalesce(a.username,'null') <> coalesce(b.username,'null') OR coalesce(a.name,'null') <> coalesce(b.name,'null')  OR a.isclosed <> b.isclosed OR a.dateclosed <> b.dateclosed OR coalesce(a.groupname,'null') <> coalesce(b.groupname,'null') OR coalesce(a.productname,'null') <> coalesce(b.productname,'null')  OR coalesce(a.severity,'null')  <> coalesce(b.severity,'null')  OR coalesce(a.status,'null')  <> coalesce(b.status,'null')  OR coalesce(a.initiation,'null')  <> coalesce(b.initiation,'null') OR coalesce(a.rootcause,'null') <> coalesce(b.rootcause,'null') OR coalesce(a.primarycustomer,'null') <> coalesce(b.primarycustomer,'null') OR coalesce(a.primarycategory_departA,'null') <> coalesce(b.primarycategory_departA,'null') OR coalesce(a.primarycategory_departB,'null') <> coalesce(b.primarycategory_departB,'null');"
        cursor.execute(FOR_UPDATED)
        results=cursor.fetchall()
        conn.commit()
    ids_required_update=[ticket[0] for ticket in results]
    print('{} cases requires update'.format(len(results)))
    print('TicketID: {}'.format(ids_required_update))

    # run update statement to update multiple fields for previously logged info
    if len(ids_required_update)>0:
        with conn.cursor() as cursor:
            MAKE_UPDATE = "UPDATE schema.ticketTable SET username = b.username,name= b.name,isclosed= b.isclosed ,dateclosed= b.dateclosed,groupname= b.groupname,productname= b.productname,severity= b.severity,status= b.status,initiation= b.initiation,rootcause= b.rootcause,primarycustomer = b.primarycustomer,primarycategory_departA = b.primarycategory_departA, primarycategory_departB = b.primarycategory_departB FROM schema.ticketTable a JOIN temp_api_data b ON a.id = b.id WHERE coalesce(a.username,'null') <> coalesce(b.username,'null') OR coalesce(a.name,'null') <> coalesce(b.name,'null')  OR a.isclosed <> b.isclosed OR a.dateclosed <> b.dateclosed OR coalesce(a.groupname,'null') <> coalesce(b.groupname,'null') OR coalesce(a.productname,'null') <> coalesce(b.productname,'null')  OR coalesce(a.severity,'null')  <> coalesce(b.severity,'null')  OR coalesce(a.status,'null')  <> coalesce(b.status,'null')  OR coalesce(a.initiation,'null')  <> coalesce(b.initiation,'null') OR coalesce(a.rootcause,'null') <> coalesce(b.rootcause,'null') OR coalesce(a.primarycustomer,'null') <> coalesce(b.primarycustomer,'null') OR coalesce(a.primarycategory_departA,'null') <> coalesce(b.primarycategory_departA,'null') OR coalesce(a.primarycategory_departB,'null') <> coalesce(b.primarycategory_departB,'null');"
    
            cursor.execute(MAKE_UPDATE)
            conn.commit()
        print('Tickets were updated')   

###### Executions ######
# connection info
database_USERNAME = os.environ['Your_database_USERNAME']
database_PASSWORD = os.environ['Your_database_PASSWORD']
database_HOST = os.environ['Your_database_HOST']
databaseT_DB = os.environ['Your_database_DB']
database_PORT = os.environ['Your_database_PORT']
org_id =  os.environ['Your_id']
access_token =  os.environ['Your_access_token']

# define the database connection
database_url = f"postgresql://{database_USERNAME}:{database_PASSWORD}@{database_HOST}:{database_PORT}/{database_DB}" 
# create a connection to database using sqlalchemy
engine = create_engine(database_url)
# valid application endpoint
standard_url = "Your valid endpoint" 
# connection
conn = psycopg2.connect(dbname=database_DB, host=database_HOST, port=database_PORT, user=database_USERNAME, password=database_PASSWORD)
cursor=conn.cursor()

###### data extraction ######
# look for max_date from last insert & latest closed date for logged tickets info update
with conn.cursor() as cursor:
    MAX_LOOKBACK_DATE = "SELECT max(ticketnumber),max(dateclosed)-3  FROM schema.ticketTable;"
    cursor.execute(MAX_LOOKBACK_DATE)
    result = cursor.fetchall()[0]

# endpoints
max_ticketnumber = result[0]
max_dateclosed= result[1].strftime('%Y%m%d%H%M%S')
new_insert_url = standard_url +'&ticketnumber[gt]=' + max_ticketnumber
recent_closed_ticket_url = standard_url +'&dateclosed=' + max_dateclosed 
recent_modified_update = standard_url +'&datemodified='+ max_dateclosed

###### Inserting newly created tickets ######
# extract new data from API call
newly_created_tickets = ticket_insert_prep(pd.DataFrame(get_ticket_data(new_insert_url)))
# load new records into the table
batch_load('ticketTable',newly_created_tickets)

#detect updates in the platform for entries logged previously and update the corresponding information in the table.
update_ticket_tb(recent_closed_ticket_url)
update_ticket_tb(recent_modified_update)

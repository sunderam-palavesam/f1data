from sqlalchemy import create_engine
import pandas as pd
import fastf1 as f1
import mysql.connector
from mysql.connector import Error

f1.Cache.enable_cache('./doc_cache')
my_conn=create_engine("mysql+mysqldb://root:Preetha143@host.docker.internal/f1data")
event_year=2022

def db_check():
    db_name=my_conn.engine.url.database
    print("connected to database: ", db_name)

    sql_create = 'CREATE TABLE if not EXISTS f1data.event_schedule ( \
                RoundNumber bigint DEFAULT NULL, \
                Country text, \
                Location text, \
                OfficialEventName text, \
                EventDate datetime DEFAULT NULL, \
                EventName text, \
                EventFormat text, \
                Session1 text, \
                Session1Date datetime DEFAULT NULL, \
                Session2 text, \
                Session2Date datetime DEFAULT NULL, \
                Session3 text, \
                Session3Date datetime DEFAULT NULL, \
                Session4 text, \
                Session4Date datetime DEFAULT NULL, \
                Session5 text, \
                Session5Date datetime DEFAULT NULL, \
                F1ApiSupport tinyint(1) DEFAULT NULL, \
                EventYear bigint DEFAULT NULL, \
                EventDWLoad text, \
                EventDWKey text \
            ) ;'
    my_conn.execute(sql_create) 

def load_event_schedule(my_event_year=event_year):
    event_schedule = f1.get_event_schedule(my_event_year)
    event_schedule_df = pd.DataFrame(event_schedule)
    event_schedule_df['EventYear'] = my_event_year
    event_schedule_df['EventDWLoad'] = 'N'        
    event_schedule_df['EventDWKey'] = (str(my_event_year) + event_schedule_df['EventName']).apply(lambda x: x.upper().replace(' ','').strip())
    event_schedule_df.to_sql(con=my_conn,name='stage_event_schedule',if_exists='replace',index=False)

    sql_insert = 'insert into f1data.event_schedule \
                SELECT * FROM f1data.stage_event_schedule s \
                where not exists (select 1 from f1data.event_schedule e where s.EventDWKey  = e.EventDWKey ) '
    
    my_conn.execute(sql_insert)    

def load_session_data(my_event_year=event_year):

    #get session to be loaded
    sql_select = 'SELECT EventYear,EventName,EventDWKey \
                FROM f1data.event_schedule \
                WHERE session5 = "Race"  and EventDWLoad = "N" \
                and EventYear = ' + str(my_event_year) + ';'
    
    event_data = my_conn.execute(sql_select).fetchall()
    for event in event_data:
        print(event['EventYear'])
        print(event['EventName'])
        print(event['EventDWKey'])

        #load a session and its  data
        race = f1.get_session(event['EventYear'], event['EventName'], 'R')
        race.load()
        race_data = pd.DataFrame(race.results)
        race_data['RaceDWKey'] = event['EventDWKey']
        race_data.to_sql(con=my_conn,name='race_data',if_exists='append',index=False)

        sql_update = 'UPDATE f1data.event_schedule SET EventDWLoad = "Y" \
                    WHERE EventDWKey = "' + event['EventDWKey'] + '";'

        my_conn.execute(sql_update)

if __name__ == "__main__":
    db_check()
    load_event_schedule(event_year)

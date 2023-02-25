from sqlalchemy import create_engine 
import pandas as pd
import fastf1 as f1
import mysql.connector
from mysql.connector import Error
#import hashlib

f1.Cache.enable_cache('C:/Sunderam/Masters/Project/airflow_docker/doc_cache')
my_conn=create_engine("mysql+mysqldb://root:Preetha143@localhost/f1data")
event_year=2022

def db_connect():
    
    #my_conn=create_engine("mysql+mysqldb://root:Preetha143@host.docker.internal/world")
    try:
        connection = mysql.connector.connect(
                                    host='localhost',
                                    database='world',
                                    user='root',
                                    password='Preetha143')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

"""
def pre_process():
    my_dict={
        'class':['five','six','three'],
        'number':[5,2,3]
    }

    df = pd.DataFrame(data=my_dict)
    print(df)

    df.to_sql(con=my_conn,name='student2',if_exists='append',index=False)
"""
def get_event_data_new():

    sqlproc = 'call eventcount() '
    
    df = my_conn.execute(sqlproc)

    print(df)

def get_event_data():
    event_schedule = f1.get_event_schedule(event_year)
    event_schedule_df = pd.DataFrame(event_schedule)
    event_schedule_df['EventYear'] = event_year
    event_schedule_df['EventDWLoad'] = 'N'    
    #event_schedule_df['EventDWKey1'] = (str(event_year) + event_schedule_df['EventName']).apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    event_schedule_df['EventDWKey'] = (str(event_year) + event_schedule_df['EventName']).apply(lambda x: x.upper().replace(' ','').strip())
    event_schedule_df.to_sql(con=my_conn,name='stage_event_data',if_exists='replace',index=False)

    #queryString = 'SELECT * FROM world.event_data WHERE EventYear = 2022 and EventDWLoad = "N"'
    #queryResultDF = pd.read_sql_query(queryString, con=my_conn)
    #print(queryResultDF)

    sqlInsert = 'insert into world.event_data \
                SELECT * FROM world.stage_event_data s \
                where not exists (select 1 from world.event_data e where s.EventDWKey  = e.EventDWKey ) '
    
    my_conn.execute(sqlInsert)

    #queryResultDF = pd.read_sql_query(queryString, con=my_conn)


    #event_schedule_df.merge(queryResultDF, how='inner', on=['EventDWKey'])

    #print(event_schedule_df)
    #rule = cr_df['rank']==1
    #cr_df[rule].merge(be_df, how='left', on=['sha256_cpn']).append(cr_df[~rule])
   


def get_session_data():

    # load a session and its telemetry data
    race = f1.get_session(2021, 'Spanish Grand Prix', 'Q')
    laps = race.load_laps(with_telemetry=True)
    #print(session.results[:3])
    print(laps)
    print(laps.columns)
    print(race.results.columns)
        
    #df = pd.DataFrame(data=laps)
    #print(df)

    laps.to_sql(con=my_conn,name='laps_tbl',if_exists='append',index=False)

def get_session_data_new():

    # load a session and its telemetry data
    race = f1.get_session(2022, 'Spanish Grand Prix', 'R')
    race.load()
    print(race.results)
    race_data = race.results
    race_data.to_sql(con=my_conn,name='race_results',if_exists='append',index=False)    
    #print(race.weather_data.columns)
    
    #print(race.weather_data)
    #weather_data = race.weather_data
    #weather_data.to_sql(con=my_conn,name='weather_data',if_exists='append',index=False)


    #print(race.laps)
    #lap_data = race.laps
    #lap_data.to_sql(con=my_conn,name='lap_data',if_exists='append',index=False)


    #print(race.race_control_messages)
    #control_messages = race.race_control_messages
    #control_messages.to_sql(con=my_conn,name='control_messages',if_exists='append',index=False)

    #print(race.event)
    #event_data = race.event
    #event_data.to_sql(con=my_conn,name='event_data',if_exists='append',index=False)

    #print(race.car_data)
    #car_data = race.car_data    
    #for driver in car_data.keys():
    #    #print(car_data[driver])
    #    car_data_df = pd.DataFrame(car_data[driver])
    #    car_data_df['Driver'] = driver
    #    car_data_df.to_sql(con=my_conn,name='car_data_df',if_exists='append',index=False)

    #print(race.pos_data)
    #pos_data = race.pos_data    
    #for driver in pos_data.keys():
    #    pos_data_df = pd.DataFrame(pos_data[driver])
    #    pos_data_df['Driver'] = driver
    #    pos_data_df.to_sql(con=my_conn,name='pos_data_df',if_exists='append',index=False)
    
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

if __name__ == "__main__":
#    pre_process()
    #db_connect()
    #get_session_data_new()
    #get_event_data()
    load_session_data()
    #get_session_data_new()

    
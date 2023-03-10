from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import fastf1 as f1

f1.Cache.enable_cache('./doc_cache')
my_conn=create_engine("mysql+mysqldb://root:Preetha143@localhost/f1data")


def db_check():
    db_name=my_conn.engine.url.database
    print("connected to database: ", db_name)

    sql_exec = 'call create_f1_db_objects() '    
    my_conn.execute(sql_exec)    

def load_event_schedule(event_year):
    my_event_year = int(event_year)
    event_schedule = f1.get_event_schedule(my_event_year)
    event_schedule_df = pd.DataFrame(event_schedule)
    event_schedule_df['EventYear'] = my_event_year
    event_schedule_df['EventDWLoad'] = 'N'        
    event_schedule_df['EventDWKey'] = (str(my_event_year) + event_schedule_df['EventName']).apply(lambda x: x.upper().replace(' ','').strip())
    event_schedule_df.to_sql(con=my_conn,name='stg_event_schedule',if_exists='replace',index=False)

    sql_insert = 'insert into f1data.event_schedule \
                SELECT * FROM f1data.stg_event_schedule s \
                where not exists (select 1 from f1data.event_schedule e where s.EventDWKey  = e.EventDWKey ) '
    
    my_conn.execute(sql_insert)    

def load_session_data(event_year):
    my_event_year = int(event_year)

    #get session to be loaded
    sql_select = 'SELECT EventYear,EventName,EventDWKey \
                FROM f1data.event_schedule \
                WHERE session5 = "Race"  and EventDWLoad = "N" \
                and EventYear = ' + str(my_event_year) + ';'
    
    event_data = my_conn.execute(sql_select).fetchall()

    #load a session and its  data
    for event in event_data:
        print(event['EventYear'])
        print(event['EventName'])
        print(event['EventDWKey'])
        eventdwkey=event['EventDWKey']

        sql_exec = 'call drop_stg_tables() '    
        my_conn.execute(sql_exec)  

        #load race results data
        race = f1.get_session(event['EventYear'], event['EventName'], 'R')
        race.load()
        race_data = pd.DataFrame(race.results).astype(str)
        race_data['EventDWKey'] = eventdwkey
        race_data.to_sql(con=my_conn,name='stg_race_data',if_exists='append',index=False)

        #load weather data
        #print(race.weather_data)
        weather_data = pd.DataFrame(race.weather_data).astype(str)
        weather_data['EventDWKey'] = eventdwkey
        weather_data.to_sql(con=my_conn,name='stg_weather_data',if_exists='append',index=False)

        #load lap data
        #print(race.laps)         
        lap_data = pd.DataFrame(race.laps).astype(str)
        lap_data['EventDWKey'] = eventdwkey
        lap_data.to_sql(con=my_conn,name='stg_lap_data',if_exists='append',index=False)

        #load race control messages
        #print(race.race_control_messages)
        #control_messages = race.race_control_messages
        #control_messages['EventDWKey'] = eventdwkey
        #control_messages.to_sql(con=my_conn,name='stg_control_messages',if_exists='append',index=False)

        #load car data
        #print(race.car_data)

        car_data = race.car_data    
        for driver in car_data.keys():        
            car_data_df = pd.DataFrame(car_data[driver]).astype(str)
            car_data_df['Driver'] = driver
            car_data_df['EventDWKey'] = eventdwkey
            car_data_df.to_sql(con=my_conn,name='stg_car_data',if_exists='append',index=False)

        """
        #load position data
        #print(race.pos_data)
        pos_data = race.pos_data    
        for driver in pos_data.keys():
            pos_data_df = pd.DataFrame(pos_data[driver]).astype(str)
            pos_data_df['Driver'] = driver
            pos_data_df['EventDWKey'] = eventdwkey
            pos_data_df.to_sql(con=my_conn,name='stg_pos_data',if_exists='append',index=False)


        sql_update = 'UPDATE f1data.event_schedule SET EventDWLoad = "Y" \
                    WHERE EventDWKey = "' + eventdwkey + '";'

        my_conn.execute(sql_update)
        """
        
        connection = my_conn.engine.raw_connection()        
        cursor_obj = connection.cursor()
        args= [eventdwkey]
        cursor_obj.callproc('load_f1_tables', args)
        cursor_obj.close()
        connection.commit()


if __name__ == "__main__":
    db_check()
    load_event_schedule(2021)
    load_session_data(2021)

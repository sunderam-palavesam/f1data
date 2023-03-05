from sqlalchemy import create_engine
import pandas as pd
import fastf1 as f1
import mysql.connector
from mysql.connector import Error

f1.Cache.enable_cache('./doc_cache')
my_conn=create_engine("mysql+mysqldb://root:Preetha143@host.docker.internal/f1data")

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
if __name__ == "__main__":
    db_check()
    load_event_schedule(2021)

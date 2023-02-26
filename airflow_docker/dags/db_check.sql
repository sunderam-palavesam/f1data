DROP PROCEDURE f1data.create_f1_db_objects;

create procedure f1data.create_f1_db_objects()
begin
	DROP TABLE if exists f1data.stg_event_schedule;
	DROP TABLE if exists f1data.stg_race_data;
	DROP TABLE if exists f1data.stg_weather_data;
	DROP TABLE if exists f1data.stg_lap_data;
	DROP TABLE if exists f1data.stg_control_messages;
	DROP TABLE if exists f1data.stg_car_data;
	DROP TABLE if exists f1data.stg_pos_data;	
	
	CREATE TABLE if not EXISTS f1data.event_schedule ( 
        RoundNumber bigint DEFAULT NULL, 
        Country text, 
        Location text, 
        OfficialEventName text, 
        EventDate datetime DEFAULT NULL, 
        EventName text, 
        EventFormat text, 
        Session1 text, 
        Session1Date datetime DEFAULT NULL, 
        Session2 text, 
        Session2Date datetime DEFAULT NULL, 
        Session3 text, 
        Session3Date datetime DEFAULT NULL, 
        Session4 text, 
        Session4Date datetime DEFAULT NULL, 
        Session5 text, 
        Session5Date datetime DEFAULT NULL, 
        F1ApiSupport tinyint(1) DEFAULT NULL, 
        EventYear bigint DEFAULT NULL, 
        EventDWLoad text, 
        EventDWKey text 
    );
   
end;

DROP PROCEDURE if exists f1data.drop_stg_tables;

create procedure f1data.drop_stg_tables()
begin
	DROP TABLE if exists f1data.stg_race_data;
	DROP TABLE if exists f1data.stg_weather_data;
	DROP TABLE if exists f1data.stg_lap_data;
	DROP TABLE if exists f1data.stg_car_data;
end;

DROP PROCEDURE if exists f1data.load_stg_events;

create procedure f1data.load_stg_events()
begin

    insert into event_schedule
    select * from stg_event_schedule s 
    where not exists (
        select 1 from event_schedule e where s.EventDWKey  = e.EventDWKey ) ;
    
    truncate table stg_event_schedule;
end;

DROP PROCEDURE if exists f1data.create_f1_db_objects;

create procedure f1data.create_f1_db_objects()
begin
	
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

	CREATE TABLE if not EXISTS f1data.race_data (    
        DriverNumber text,
        BroadcastName text,
        Abbreviation text,
        TeamName text,
        TeamColor text,
        FirstName text,
        LastName text,
        FullName text,
        Position text,
        GridPosition text,  
        RaceTime text,
        RaceStatus text,
        Points text,
        EventDWKey text
    );

	CREATE TABLE if not EXISTS f1data.weather_data (    
        AirTemp text,
        Humidity text,
        Pressure text,
        Rainfall text,
		TrackTemp text,
		WindSpeed text,
        EventDWKey text
    );

    CREATE TABLE if not EXISTS f1data.lap_data (    
        RaceTime text,
        DriverNumber text,
        LapTime text,
        LapNumber text,
        PitOutTime text,
        PitInTime text,
        Sector1Time text,
        Sector2Time text,
        Sector3Time text,
        Sector1SessionTime text,
        Sector2SessionTime text,
        Sector3SessionTime text,
        SpeedI1 text,
        SpeedI2 text,
        SpeedFL text,
        SpeedST text,
        IsPersonalBest text,
        Compound text,
        TyreLife text,
        FreshTyre text,
        Stint text,
        LapStartTime text,
        Team text,
        Driver text,
        TrackStatus text,
        IsAccurate text,
        LapStartDate text,
        EventDWKey text
    ) ;

    CREATE TABLE if not EXISTS f1data.car_data (
        RaceDate text,
        RPM text,
        Speed text,
        nGear text,
        Throttle text,
        Brake text,
        DRS text,
        Source text,
        CarTime text,
        SessionTime text,
        Driver text,
        EventDWKey text
    ) ;

end;

DROP PROCEDURE if exists f1data.load_f1_tables;


create procedure f1data.load_f1_tables(IN EventKey VARCHAR(255))
begin
	
    delete from race_data where EventDWKey = EventKey ;
    delete from weather_data where EventDWKey = EventKey ;
    delete from lap_data where EventDWKey = EventKey ;
    delete from car_data where EventDWKey = EventKey ;

    insert into race_data ( 
	    DriverNumber , 
        BroadcastName , 
        Abbreviation , 
        TeamName , 
        TeamColor ,
	    FirstName ,  
        LastName ,  
        FullName ,  
        Position ,  
        GridPosition ,  
	    RaceTime ,  
        RaceStatus ,  
        Points ,  
        EventDWKey 
    )
    select   
	    DriverNumber ,  
        BroadcastName ,  
        Abbreviation ,  
        TeamName ,  
        TeamColor ,
	    FirstName ,  
        LastName ,  
        FullName ,  
        Position ,  
        GridPosition ,  
	    Time ,  
        Status ,  
        Points ,  
        EventDWKey 
    from stg_race_data ;

    insert into weather_data(
        AirTemp ,
        Humidity ,
        Pressure ,
        Rainfall ,
		TrackTemp,
		WindSpeed,
        EventDWKey
    )
    select 
        avg(AirTemp) , 
        avg(Humidity) , 
        avg(Pressure), 
        max(case when Rainfall = 'False' then 'False' else 'True' end) ,
        avg(TrackTemp), 
        avg(WindSpeed), 
        EventDWKey
    from stg_weather_data
    group by EventDWKey ;

    insert into lap_data (
        RaceTime ,
        DriverNumber ,
        LapTime ,
        LapNumber ,
        PitOutTime ,
        PitInTime ,
        Sector1Time ,
        Sector2Time ,
        Sector3Time ,
        Sector1SessionTime ,
        Sector2SessionTime ,
        Sector3SessionTime ,
        SpeedI1 ,
        SpeedI2 ,
        SpeedFL ,
        SpeedST ,
        IsPersonalBest ,
        Compound ,
        TyreLife ,
        FreshTyre ,
        Stint ,
        LapStartTime ,
        Team ,
        Driver ,
        TrackStatus ,
        IsAccurate ,
        LapStartDate ,
        EventDWKey 
    )
    select 
        Time ,
        DriverNumber ,
        LapTime ,
        LapNumber ,
        PitOutTime ,
        PitInTime ,
        Sector1Time ,
        Sector2Time ,
        Sector3Time ,
        Sector1SessionTime ,
        Sector2SessionTime ,
        Sector3SessionTime ,
        SpeedI1 ,
        SpeedI2 ,
        SpeedFL ,
        SpeedST ,
        IsPersonalBest ,
        Compound ,
        TyreLife ,
        FreshTyre ,
        Stint ,
        LapStartTime ,
        Team ,
        Driver ,
        TrackStatus ,
        IsAccurate ,
        LapStartDate ,
        EventDWKey 
    from stg_lap_data ;

    insert into car_data (
        RaceDate ,
        RPM ,
        Speed ,
        nGear ,
        Throttle ,
        Brake ,
        DRS ,
        Source ,
        CarTime ,
        SessionTime ,
        Driver ,
        EventDWKey 
    ) 
    select 
        Date ,
        RPM ,
        Speed ,
        nGear ,
        Throttle ,
        Brake ,
        DRS ,
        Source ,
        Time ,
        SessionTime ,
        Driver ,
        EventDWKey 
    from stg_car_data
    where nGear > 0 and Speed > 0 ;

    update event_schedule set EventDWLoad = 'Y' WHERE EventDWKey = EventKey ;


end ;



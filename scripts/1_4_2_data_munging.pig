/* Reads the raw data and eppends the UUID and the timestamp to each record and then stores in orc format*/

/* Airports raw data to ORC format */
rawdata = LOAD '/data/raw/airports' USING PigStorage(',') AS (iata:chararray,airport:chararray,city:chararray,state:chararray,country:chararray,lat:float,long:float);
withUUID = RANK rawdata BY iata ASC, airport ASC, city ASC;
DESCRIBE withUUID;
currentTS = CurrentTime() AS (ts: datetime);
withts = FOREACH withUUID GENERATE rank,iata,airport,city,state,country,lat,long,CurrentTS.ts;
STORE withts INTO '/data/decomposed/airports' USING AvroStorage();


/* Carriers raw data to ORC format */
rawdata = LOAD '/data/raw/carriers' USING PigStorage(',') AS (Code:chararray,Description:chararray);
withUUID = RANK rawdata BY Code ASC, Description ASC;
DESCRIBE withUUID;
currentTS = CurrentTime() AS (ts: datetime);
withts = FOREACH withUUID GENERATE rank, Code, Description, currentTS.ts;
STORE withts INTO '/data/decomposed/carriers' USING AvroStorage();


/* Planes raw data to ORC format */
rawdata = LOAD '/data/raw/planedate' USING PigStorage(',') AS (tailnum:chararray,type:chararray, manufacturer:chararray, issue_date:chararray, model:chararray, status:chararray, aircraft_type:chararray, engine_type:chararray, year:int);
withUUID = RANK rawdata BY tailnum ASC;
DESCRIBE withUUID;
currentTS = CurrentTime() AS (ts: datetime);
withts = FOREACH withUUID GENERATE rank, tailnum, type, manufacturer, issue_date, model, status, aircraft_type, engine_type, year, currentTS.ts;
STORE withts INTO '/data/decomposed/planedate' USING AvroStorage();


/* OTP raw data to ORC  files */
rawdata = LOAD '/data/raw/otp' USING PigStorage(',') AS (Year:int,Month:int,DayofMonth:int,DayOfWeek:int,DepTime:int,CRSDepTime:int,ArrTime:int,CRSArrTime:int,UniqueCarrier:chararray,FlightNum:int,TailNum:chararray,ActualElapsedTime:int,CRSElapsedTime:int,AirTime:int,ArrDelay:int,DepDelay:int,Origin:chararray,Dest:chararray,Distance:int,TaxiIn:int,TaxiOut:int,Cancelled:chararray,CancellationCode:chararray,Diverted:int,CarrierDelay:int,WeatherDelay:int,NASDelay:int,SecurityDelay:int,LateAircraftDelay:int);
withUUID = RANK rawdata BY TailNum ASC, Origin ASC;
DESCRIBE withUUID;
currentTS = CurrentTime() AS (ts: datetime);
withts = FOREACH withUUID GENERATE rank,Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,currentTS.ts;
STORE withts INTO '/data/decomposed/otp' USING AvroStorage();

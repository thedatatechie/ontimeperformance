create database emirates_db;
use emirates_db;

CREATE EXTERNAL TABLE ht_aiports(
rank int,
iata string,
airport string,
city string,
state string,
country string,
lat double,
long double,
ts timestamp)
STORED AS PARQUET
LOCATION
'/user/vijaya/data/modelled/airports';



CREATE EXTERNAL TABLE ht_planedate(
rank int,
tailnum string,
type string,
manufacturer string,
issue_date string,
model string,
status string,
aircraft_type string,
engine_type string,
year int,
ts timestamp
)
STORED AS PARQUET
LOCATION
'/user/vijaya/data/modelled/planedate';



CREATE EXTERNAL TABLE ht_carriers(
rank int,
Code string,
Description string,
ts timestamp
)
STORED AS PARQUET
LOCATION
'/user/vijaya/data/modelled/carriers';





CREATE EXTERNAL TABLE ht_otp(
rank int,
Year int,
Month int,
DayofMonth int,
DayOfWeek int,
DepTime int,
CRSDepTime int,
ArrTime int,
CRSArrTime int,
UniqueCarrier string,
FlightNum int,
TailNum string,
ActualElapsedTime int,
CRSElapsedTime int,
AirTime int,
ArrDelay int,
DepDelay int,
Origin string,
Dest string,
Distance int,
TaxiIn int,
TaxiOut int,
Cancelled string,
CancellationCode string,
Diverted int,
CarrierDelay int,
WeatherDelay int,
NASDelay int,
SecurityDelay int,
LateAircraftDelay int,
ts timestamp
)
STORED AS PARQUET
LOCATION
'/user/vijaya/data/modelled/otp';

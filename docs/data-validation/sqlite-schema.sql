CREATE TABLE IF NOT EXISTS "airports" (
"airport_code" TEXT,
  "icao_code" TEXT,
  "airport_name" TEXT,
  "type" TEXT,
  "latitude" REAL,
  "longitude" REAL,
  "elevation_ft" REAL,
  "country" TEXT,
  "region" TEXT,
  "municipality" TEXT,
  "scheduled_service" TEXT
);
CREATE TABLE weather_hourly (
        airport_code TEXT,
        weather_datetime TEXT,
        weather_date TEXT,
        weather_hour INTEGER,
        latitude REAL,
        longitude REAL,
        temperature_2m REAL,
        precipitation REAL,
        rain REAL,
        snowfall REAL,
        wind_speed_10m REAL,
        wind_gusts_10m REAL,
        visibility REAL,
        cloud_cover REAL,
        PRIMARY KEY (airport_code, weather_datetime)
    );
CREATE TABLE raw_ontime_flights (
    Year INTEGER,
    Quarter INTEGER,
    Month INTEGER,
    DayofMonth INTEGER,
    DayOfWeek INTEGER,
    FlightDate TEXT,

    Reporting_Airline TEXT,
    DOT_ID_Reporting_Airline INTEGER,
    IATA_CODE_Reporting_Airline TEXT,
    Tail_Number TEXT,
    Flight_Number_Reporting_Airline INTEGER,

    OriginAirportID INTEGER,
    OriginAirportSeqID INTEGER,
    OriginCityMarketID INTEGER,
    Origin TEXT,
    OriginCityName TEXT,
    OriginState TEXT,
    OriginStateFips INTEGER,
    OriginStateName TEXT,
    OriginWac INTEGER,

    DestAirportID INTEGER,
    DestAirportSeqID INTEGER,
    DestCityMarketID INTEGER,
    Dest TEXT,
    DestCityName TEXT,
    DestState TEXT,
    DestStateFips INTEGER,
    DestStateName TEXT,
    DestWac INTEGER,

    CRSDepTime INTEGER,
    DepTime REAL,
    DepDelay REAL,
    DepDelayMinutes REAL,
    DepDel15 REAL,
    DepartureDelayGroups REAL,
    DepTimeBlk TEXT,
    TaxiOut REAL,
    WheelsOff REAL,

    WheelsOn REAL,
    TaxiIn REAL,
    CRSArrTime INTEGER,
    ArrTime REAL,
    ArrDelay REAL,
    ArrDelayMinutes REAL,
    ArrDel15 REAL,
    ArrivalDelayGroups REAL,
    ArrTimeBlk TEXT,

    Cancelled REAL,
    CancellationCode TEXT,
    Diverted REAL,

    CRSElapsedTime REAL,
    ActualElapsedTime REAL,
    AirTime REAL,
    Flights REAL,
    Distance REAL,
    DistanceGroup INTEGER,

    CarrierDelay REAL,
    WeatherDelay REAL,
    NASDelay REAL,
    SecurityDelay REAL,
    LateAircraftDelay REAL,

    FirstDepTime REAL,
    TotalAddGTime REAL,
    LongestAddGTime REAL,

    DivAirportLandings INTEGER,
    DivReachedDest REAL,
    DivActualElapsedTime REAL,
    DivArrDelay REAL,
    DivDistance REAL,

    Div1Airport TEXT,
    Div1AirportID REAL,
    Div1AirportSeqID REAL,
    Div1WheelsOn REAL,
    Div1TotalGTime REAL,
    Div1LongestGTime REAL,
    Div1WheelsOff REAL,
    Div1TailNum TEXT,

    Div2Airport TEXT,
    Div2AirportID REAL,
    Div2AirportSeqID REAL,
    Div2WheelsOn REAL,
    Div2TotalGTime REAL,
    Div2LongestGTime REAL,
    Div2WheelsOff REAL,
    Div2TailNum TEXT,

    Div3Airport TEXT,
    Div3AirportID REAL,
    Div3AirportSeqID REAL,
    Div3WheelsOn REAL,
    Div3TotalGTime REAL,
    Div3LongestGTime REAL,
    Div3WheelsOff REAL,
    Div3TailNum TEXT,

    Div4Airport TEXT,
    Div4AirportID REAL,
    Div4AirportSeqID REAL,
    Div4WheelsOn REAL,
    Div4TotalGTime REAL,
    Div4LongestGTime REAL,
    Div4WheelsOff REAL,
    Div4TailNum TEXT,

    Div5Airport TEXT,
    Div5AirportID REAL,
    Div5AirportSeqID REAL,
    Div5WheelsOn REAL,
    Div5TotalGTime REAL,
    Div5LongestGTime REAL,
    Div5WheelsOff REAL,
    Div5TailNum TEXT,

    source_year INTEGER,
    source_month INTEGER,
    CRSDepHour INTEGER,
    CRSArrHour INTEGER
);
CREATE INDEX idx_flights_origin_date_hour
ON raw_ontime_flights(Origin, FlightDate, CRSDepHour);
CREATE INDEX idx_weather_airport_date_hour
ON weather_hourly(airport_code, weather_date, weather_hour);
CREATE INDEX idx_flights_year_month
ON raw_ontime_flights(Year, Month);
CREATE INDEX idx_flights_delay_target
ON raw_ontime_flights(DepDel15);
CREATE INDEX idx_airports_code
ON airports(airport_code);

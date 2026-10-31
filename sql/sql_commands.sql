-- Run these queries after create_views(store, connection). Read committed
-- manifest paths rather than globbing all Parquet generations.

SELECT Year, Month, COUNT(*) AS flight_rows
FROM raw_ontime_flights
GROUP BY Year, Month
ORDER BY Year, Month;

SELECT airport_code, substr(weather_date, 1, 4) AS weather_year,
       COUNT(*) AS hourly_rows
FROM weather_hourly
GROUP BY airport_code, substr(weather_date, 1, 4)
ORDER BY airport_code, weather_year;

SELECT rowid, Year, Month, Origin, Dest, Cancelled, DepDel15, Diverted
FROM raw_ontime_flights
WHERE rowid % 10 = 0
ORDER BY rowid
LIMIT 10;

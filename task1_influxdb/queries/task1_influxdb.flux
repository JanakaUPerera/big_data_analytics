// Daily average temperature for Fairbanks
from(bucket: "fairbanks-climate")
  |> range(start: 2026-08-01T00:00:00Z, stop: 2026-08-18T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "hourly_climate")
  |> filter(fn: (r) => r.location == "Fairbanks")
  |> filter(fn: (r) => r._field == "temperature_2m")
  |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)

// Daily total precipitation query
from(bucket: "fairbanks-climate")
  |> range(start: 2026-08-01T00:00:00Z, stop: 2026-08-18T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "hourly_climate")
  |> filter(fn: (r) => r.location == "Fairbanks")
  |> filter(fn: (r) => r._field == "precipitation")
  |> aggregateWindow(every: 1d, fn: sum, createEmpty: false)

// Daily maximum wind gusts
from(bucket: "fairbanks-climate")
  |> range(start: 2026-08-01T00:00:00Z, stop: 2026-08-18T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "hourly_climate")
  |> filter(fn: (r) => r.location == "Fairbanks")
  |> filter(fn: (r) => r._field == "wind_gusts_10m")
  |> aggregateWindow(every: 1d, fn: max, createEmpty: false)

// Daily average surface pressure
from(bucket: "fairbanks-climate")
  |> range(start: 2026-08-01T00:00:00Z, stop: 2026-08-18T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "hourly_climate")
  |> filter(fn: (r) => r.location == "Fairbanks")
  |> filter(fn: (r) => r._field == "surface_pressure")
  |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)

// Daily average cloud cover
from(bucket: "fairbanks-climate")
  |> range(start: 2026-08-01T00:00:00Z, stop: 2026-08-18T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "hourly_climate")
  |> filter(fn: (r) => r.location == "Fairbanks")
  |> filter(fn: (r) => r._field == "cloud_cover")
  |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)

// Daily average wind speed
from(bucket: "fairbanks-climate")
  |> range(start: 2026-08-01T00:00:00Z, stop: 2026-08-18T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "hourly_climate")
  |> filter(fn: (r) => r.location == "Fairbanks")
  |> filter(fn: (r) => r._field == "wind_speed_10m")
  |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)

// Daily minimum and maximum temperature
data =
    from(bucket: "fairbanks-climate")
        |> range(start: 2026-08-01T00:00:00Z, stop: 2026-08-18T00:00:00Z)
        |> filter(fn: (r) => r._measurement == "hourly_climate")
        |> filter(fn: (r) => r.location == "Fairbanks")
        |> filter(fn: (r) => r._field == "temperature_2m")

dailyMin =
    data
        |> aggregateWindow(every: 1d, fn: min, createEmpty: false)
        |> set(key: "_field", value: "daily_min_temperature")

dailyMax =
    data
        |> aggregateWindow(every: 1d, fn: max, createEmpty: false)
        |> set(key: "_field", value: "daily_max_temperature")

union(tables: [dailyMin, dailyMax])

//  Sliding 3-hour average temperature
from(bucket: "fairbanks-climate")
  |> range(start: 2015-01-01T00:00:00Z, stop: 2026-08-18T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "hourly_climate")
  |> filter(fn: (r) => r.location == "Fairbanks")
  |> filter(fn: (r) => r._field == "temperature_2m")
  |> movingAverage(n: 3)

// Anomaly isolation using two standard deviations
data =
  from(bucket: "fairbanks-climate")
    |> range(start: 2015-01-01T00:00:00Z, stop: 2026-08-18T00:00:00Z)
    |> filter(fn: (r) => r._measurement == "hourly_climate")
    |> filter(fn: (r) => r.location == "Fairbanks")
    |> filter(fn: (r) => r._field == "temperature_2m")

meanValue =
  data
    |> mean()
    |> findRecord(fn: (key) => true, idx: 0)

stdValue =
  data
    |> stddev()
    |> findRecord(fn: (key) => true, idx: 0)

upper = meanValue._value + (2.0 * stdValue._value)
lower = meanValue._value - (2.0 * stdValue._value)

data
  |> filter(fn: (r) => r._value > upper or r._value < lower)

// Continuous downsampling into the 30-day bucket
from(bucket: "fairbanks-climate")
    |> range(start: -2d, stop: -1d)
    |> filter(fn: (r) => r._measurement == "hourly_climate")
    |> filter(fn: (r) => r.location == "Fairbanks")
    |> filter(fn: (r) => r._field == "temperature_2m")
    |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
    |> set(key: "_measurement", value: "daily_climate")
    |> to(bucket: "fairbanks-climate-downsampled", org: "bigdata-coursework")
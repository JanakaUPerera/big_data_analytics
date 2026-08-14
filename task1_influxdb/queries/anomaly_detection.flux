data =
    from(bucket: "fairbanks-airport-wind")
        |> range(
            start: 1980-01-01T00:00:00Z,
            stop: 2020-01-01T00:00:00Z
        )
        |> filter(fn: (r) =>
            r._measurement == "airport_wind" and
            r.station == "PAFA" and
            r._field == "wind_speed"
        )
        |> group(columns: [])

meanRecord =
    data
        |> mean()
        |> findRecord(
            fn: (key) => true,
            idx: 0
        )

stdRecord =
    data
        |> stddev()
        |> findRecord(
            fn: (key) => true,
            idx: 0
        )

upperThreshold =
    meanRecord._value + (2.0 * stdRecord._value)

lowerThreshold =
    meanRecord._value - (2.0 * stdRecord._value)

data
    |> filter(fn: (r) =>
        r._value > upperThreshold or
        r._value < lowerThreshold
    )
    |> yield(name: "wind_speed_anomalies")
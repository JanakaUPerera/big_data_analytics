from(bucket: "fairbanks-airport-wind")
    |> range(
        start: 1980-01-01T00:00:00Z,
        stop: 1981-01-01T00:00:00Z
    )
    |> filter(fn: (r) =>
        r._measurement == "airport_wind" and
        r.station == "PAFA" and
        r._field == "wind_speed"
    )
    |> aggregateWindow(
        every: 1h,
        fn: mean,
        createEmpty: false,
        timeSrc: "_start"
    )
    |> yield(name: "hourly_mean_wind_speed")
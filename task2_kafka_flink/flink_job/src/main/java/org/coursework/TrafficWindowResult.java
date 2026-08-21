package org.coursework;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TrafficWindowResult {

    public int atd_device_id;
    public long window_start;
    public long window_end;
    public int total_vehicle_count;

    public TrafficWindowResult() {
    }

    public TrafficWindowResult(
            int atd_device_id,
            long window_start,
            long window_end,
            int total_vehicle_count) {
        this.atd_device_id = atd_device_id;
        this.window_start = window_start;
        this.window_end = window_end;
        this.total_vehicle_count = total_vehicle_count;
    }

    @Override
    public String toString() {

        DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneOffset.UTC);

        String startTime = formatter.format(Instant.ofEpochMilli(window_start));

        String endTime = formatter.format(Instant.ofEpochMilli(window_end));

        return "TrafficWindowResult{" +
                "atd_device_id=" + atd_device_id +
                ", window_start='" + startTime + '\'' +
                ", window_end='" + endTime + '\'' +
                ", total_vehicle_count=" + total_vehicle_count +
                '}';
    }
}
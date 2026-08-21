package org.coursework;

public class TrafficEvent {
    public String record_id;
    public int atd_device_id;
    public String read_date;
    public String intersection_name;
    public String direction;
    public String movement;
    public boolean heavy_vehicle;
    public int volume;
    public double speed_average;
    public int bin_duration;

    // Required by Jackson/Flink
    public TrafficEvent() {
    }

    @Override
    public String toString() {
        return "TrafficEvent{" +
                "record_id='" + record_id + '\'' +
                ", atd_device_id=" + atd_device_id +
                ", read_date='" + read_date + '\'' +
                ", intersection_name='" + intersection_name + '\'' +
                ", direction='" + direction + '\'' +
                ", movement='" + movement + '\'' +
                ", heavy_vehicle=" + heavy_vehicle +
                ", volume=" + volume +
                ", speed_average=" + speed_average +
                ", bin_duration=" + bin_duration +
                '}';
    }
}

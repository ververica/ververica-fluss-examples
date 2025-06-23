package com.ververica.models;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class SensorDomain {
    public record SensorReading(
            int sensorId,
            LocalDateTime timestamp,
            double temperature,
            double humidity,
            double pressure,
            double batteryLevel
    ) {}

    public record SensorInfo(
            int sensorId,
            String name,
            String type,
            String location,
            LocalDate installationDate,
            String state,
            LocalDateTime lastUpdated
    ) {}
}

package com.ververica.utils;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataTypes;
import com.ververica.models.SensorDomain.SensorReading;
import com.ververica.models.SensorDomain.SensorInfo;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class AppUtils {

    public static final String BOOTSTRAP_SERVERS = "localhost:9123";

    public static final String IOT_HOME_DB = "sensors_db";

    public static final String SENSOR_READINGS_TBL = "sensor_readings_tbl";
    public static final String SENSOR_INFORMATION_TBL = "sensor_info";

    private static TablePath readingsTablePath = new TablePath(IOT_HOME_DB, SENSOR_READINGS_TBL);
    private static TablePath sensorInfoTablePath = new TablePath(IOT_HOME_DB, SENSOR_INFORMATION_TBL);


    public static final List<SensorReading> readings = List.of(
            new SensorReading(1, LocalDateTime.of(2025, 6, 23, 9, 15), 22.5, 45.0, 1013.2, 87.5),
            new SensorReading(2, LocalDateTime.of(2025, 6, 23, 9, 30), 23.1, 44.5, 1013.1, 88.0),
            new SensorReading(3, LocalDateTime.of(2025, 6, 23, 9, 45), 21.8, 46.2, 1012.9, 86.9),
            new SensorReading(4, LocalDateTime.of(2025, 6, 23, 10, 0), 24.0, 43.8, 1013.5, 89.2),
            new SensorReading(5, LocalDateTime.of(2025, 6, 23, 10, 15), 22.9, 45.3, 1013.0, 87.8),
            new SensorReading(6, LocalDateTime.of(2025, 6, 23, 10, 30), 23.4, 44.9, 1013.3, 88.3),
            new SensorReading(7, LocalDateTime.of(2025, 6, 23, 10, 45), 21.7, 46.5, 1012.8, 86.5),
            new SensorReading(8, LocalDateTime.of(2025, 6, 23, 11, 0), 24.2, 43.5, 1013.6, 89.5),
            new SensorReading(9, LocalDateTime.of(2025, 6, 23, 11, 15), 23.0, 45.1, 1013.2, 87.9),
            new SensorReading(10, LocalDateTime.of(2025, 6, 23, 11, 30), 22.6, 45.7, 1013.0, 87.4)
    );

    public static final List<SensorInfo> sensorInfos = List.of(
            new SensorInfo(1, "Outdoor Temp Sensor", "Temperature", "Roof", LocalDate.of(2024, 1, 15), "OK", LocalDateTime.of(2025, 6, 23, 9, 15)),
            new SensorInfo(2, "Main Lobby Sensor", "Humidity", "Lobby", LocalDate.of(2024, 2, 20), "ERROR", LocalDateTime.of(2025, 6, 23, 9, 30)),
            new SensorInfo(3, "Server Room Sensor", "Temperature", "Server Room", LocalDate.of(2024, 3, 10), "MAINTENANCE", LocalDateTime.of(2025, 6, 23, 9, 45)),
            new SensorInfo(4, "Warehouse Sensor", "Pressure", "Warehouse", LocalDate.of(2024, 4, 5), "OK", LocalDateTime.of(2025, 6, 23, 10, 0)),
            new SensorInfo(5, "Conference Room Sensor", "Humidity", "Conference Room", LocalDate.of(2024, 5, 25), "OK", LocalDateTime.of(2025, 6, 23, 10, 15)),
            new SensorInfo(6, "Office 1 Sensor", "Temperature", "Office 1", LocalDate.of(2024, 6, 18), "LOW_BATTERY", LocalDateTime.of(2025, 6, 23, 10, 30)),
            new SensorInfo(7, "Office 2 Sensor", "Humidity", "Office 2", LocalDate.of(2024, 7, 12), "OK", LocalDateTime.of(2025, 6, 23, 10, 45)),
            new SensorInfo(8, "Lab Sensor", "Temperature", "Lab", LocalDate.of(2024, 8, 30), "ERROR", LocalDateTime.of(2025, 6, 23, 11, 0)),
            new SensorInfo(9, "Parking Lot Sensor", "Pressure", "Parking Lot", LocalDate.of(2024, 9, 14), "OK", LocalDateTime.of(2025, 6, 23, 11, 15)),
            new SensorInfo(10, "Backyard Sensor", "Temperature", "Backyard", LocalDate.of(2024, 10, 3), "OK", LocalDateTime.of(2025, 6, 23, 11, 30)),

            // SEND SOME UPDATES
            new SensorInfo(2, "Main Lobby Sensor", "Humidity", "Lobby", LocalDate.of(2024, 2, 20), "ERROR", LocalDateTime.of(2025, 6, 23, 9, 48)),
            new SensorInfo(8, "Lab Sensor", "Temperature", "Lab", LocalDate.of(2024, 8, 30), "ERROR", LocalDateTime.of(2025, 6, 23, 11, 16))
    );


    public static Connection getConnection() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), BOOTSTRAP_SERVERS);


        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    public static Schema getSensorReadingsSchema() {
        return Schema.newBuilder()
                .column("sensorId", DataTypes.INT())
                .column("timestamp", DataTypes.TIMESTAMP())
                .column("temperature", DataTypes.DOUBLE())
                .column("humidity", DataTypes.DOUBLE())
                .column("pressure", DataTypes.DOUBLE())
                .column("batteryLevel", DataTypes.DOUBLE())
                .build();
    }

    public static Schema getSensorInfoSchema() {
        return Schema.newBuilder()
                .column("sensorId", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("type", DataTypes.STRING())
                .column("location", DataTypes.STRING())
                .column("installationDate", DataTypes.DATE())
                .column("state", DataTypes.STRING())
                .column("lastUpdated", DataTypes.TIMESTAMP())
                .primaryKey("sensorId")
                .build();
    }

    public static void setupTables(Admin admin) throws ExecutionException, InterruptedException {
        TableDescriptor readingsDescriptor = TableDescriptor.builder()
                .schema(getSensorReadingsSchema())
                .distributedBy(3, "sensorId")
                .comment("This is the sensor readings table")
                .build();

        admin.dropTable(readingsTablePath, true).get();
        admin.dropTable(sensorInfoTablePath, true).get();

        admin.createTable(readingsTablePath, readingsDescriptor, true).get();

        TableDescriptor sensorInfoDescriptor = TableDescriptor.builder()
                .schema(getSensorInfoSchema())
                .distributedBy(3, "sensorId")
                .comment("This is the sensor information table")
                .build();

        admin.createTable(sensorInfoTablePath, sensorInfoDescriptor, true).get();
    }

    public static TablePath getSensorReadingsTablePath() {
        return readingsTablePath;
    }

    public static TablePath getSensorInfoTablePath() {
        return sensorInfoTablePath;
    }
}

package com.ververica.writer;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.DatabaseInfo;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.TimestampNtz;
import com.ververica.models.SensorDomain;
import com.ververica.utils.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ververica.models.SensorDomain.SensorInfo;
import com.ververica.models.SensorDomain.SensorReading;

import java.sql.Timestamp;
import java.util.concurrent.ExecutionException;

public class FlussWriter {
    private static final Logger logger = LoggerFactory.getLogger(FlussWriter.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("Starting {}", FlussWriter.class.getSimpleName());

        Connection connection = AppUtils.getConnection();
        Admin admin = connection.getAdmin();

        admin.listDatabases().get().forEach(System.out::println);

        DatabaseDescriptor descriptor =
                DatabaseDescriptor.builder()
                        .comment("This is home sensors database")
                        .customProperty("owner", "Dave")
                        .build();

        admin.createDatabase(AppUtils.IOT_HOME_DB, descriptor, true).get();

        admin.listDatabases().get().forEach(System.out::println);
        DatabaseInfo dbInfo = admin.getDatabaseInfo(AppUtils.IOT_HOME_DB).get();
        logger.info("Database info: {}", new String(dbInfo.getDatabaseDescriptor().toJsonBytes()));

        AppUtils.setupTables(admin);
        logger.info("Tables: '{}' and '{}' created successfully", AppUtils.SENSOR_READINGS_TBL, AppUtils.SENSOR_INFORMATION_TBL);


        logger.info("Creating table writer for table {} ...", AppUtils.SENSOR_READINGS_TBL);
        Table table = connection.getTable(AppUtils.getSensorReadingsTablePath());
        AppendWriter writer = table.newAppend().createWriter();

        AppUtils.readings.forEach(reading -> {
            GenericRow row = energyReadingToRow(reading);
            writer.append(row);
        });
        writer.flush();

        logger.info("Sensor Readings Written Successfully.");

        logger.info("Creating table writer for table {} ...", AppUtils.SENSOR_INFORMATION_TBL);
        Table sensorInfoTable = connection.getTable(AppUtils.getSensorInfoTablePath());
        UpsertWriter upsertWriter = sensorInfoTable.newUpsert().createWriter();

        AppUtils.sensorInfos.forEach(sensorInfo -> {
            GenericRow row = sensorInfoToRow(sensorInfo);
            upsertWriter.upsert(row);
        });
        upsertWriter.flush();

        logger.info("Sensor Information Successfully.");


        logger.info("Closing writers and connections.");

        try {
            admin.close();
            connection.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static GenericRow energyReadingToRow(SensorReading reading) {
        GenericRow row = new GenericRow(SensorReading.class.getDeclaredFields().length);
        row.setField(0, reading.sensorId());
        row.setField(1, TimestampNtz.fromLocalDateTime(reading.timestamp()));
        row.setField(2, reading.temperature());
        row.setField(3, reading.humidity());
        row.setField(4, reading.pressure());
        row.setField(5, reading.batteryLevel());
        return row;
    }

    public static GenericRow sensorInfoToRow(SensorInfo sensorInfo) {
        GenericRow row = new GenericRow(SensorInfo.class.getDeclaredFields().length);
        row.setField(0, sensorInfo.sensorId());
        row.setField(1, BinaryString.fromString(sensorInfo.name()));
        row.setField(2, BinaryString.fromString(sensorInfo.type()));
        row.setField(3, BinaryString.fromString(sensorInfo.location()));
        row.setField(4, sensorInfo.installationDate());
        row.setField(5, BinaryString.fromString(sensorInfo.state()));
        row.setField(6, TimestampNtz.fromLocalDateTime(sensorInfo.lastUpdated()));
        System.out.println(row);
        return row;
    }
}

package com.ververica.scanner;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.lookup.LookupResult;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.ververica.utils.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;

import com.ververica.models.SensorDomain.SensorReading;


public class FlussScanner {
    private static final Logger logger = LoggerFactory.getLogger(FlussScanner.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("Starting {}", FlussScanner.class.getSimpleName());

        Connection connection = AppUtils.getConnection();
        Admin admin = connection.getAdmin();


        TablePath readingsTablePath = AppUtils.getSensorReadingsTablePath();
        TablePath sensorInfoTablePath = AppUtils.getSensorInfoTablePath();

        TableInfo tableInfo = admin.getTableInfo(readingsTablePath).get();
        logger.info("Table info: {}", tableInfo.toString());

        Table readingsTable = connection.getTable(readingsTablePath);
        Table sensorInfoTable = connection.getTable(sensorInfoTablePath);


        LogScanner logScanner = readingsTable.newScan()
//                .project(List.of())
                .createLogScanner();

        Lookuper sensorInforLookuper = sensorInfoTable
                .newLookup()
                .createLookuper();

        int numBuckets = readingsTable.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logger.info("Subscribing to Bucket {}:", i);
            logScanner.subscribeFromBeginning(i);
        }

        while (true) {
            logger.info("Polling for records...");
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    InternalRow row = record.getRow();

                    LookupResult lookupResult = sensorInforLookuper.lookup(row).get();
                    System.out.println(lookupResult.getRowList());

                    SensorReading reading = new SensorReading(
                            row.getInt(0),
                            LocalDateTime.parse(row.getTimestampNtz(2, 6).toString(), formatter),
                            row.getDouble(3),
                            row.getDouble(4),
                            row.getDouble(5),
                            row.getDouble(6)
                    );
                    logger.info("Reading: {}", reading);
                    logger.info("Bucket: {} \tRecord: {}", bucket, row);
                }
            }
        }
    }
}


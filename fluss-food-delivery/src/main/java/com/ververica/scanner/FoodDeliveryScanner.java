package com.ververica.scanner;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.ververica.utils.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ververica.models.FoodDeliveryDomain.OrderPaymentInfo;

import java.time.Duration;
import java.util.List;

public class FoodDeliveryScanner {
    private static final Logger logger = LoggerFactory.getLogger(FoodDeliveryScanner.class);

    public record OrderPaymentInfoPartial(String orderId, double totalAmount, String paymentMethod) {}

    public static void main(String[] args) {
        logger.info("Starting {}", FoodDeliveryScanner.class.getSimpleName());
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), AppUtils.BOOTSTRAP_SERVERS);
        Connection connection = ConnectionFactory.createConnection(conf);

        TablePath tablePath = OrderPaymentInfo.tablePath();
        Table orderPaymentTable = connection.getTable(tablePath);

        LogScanner logScanner = orderPaymentTable.newScan()
                .project(List.of("orderId", "totalAmount", "paymentMethod"))
                .createLogScanner();

        int numBuckets = orderPaymentTable.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logger.info("Subscribing to Bucket {}.", i);
            logScanner.subscribeFromBeginning(i);
        }

        while (true) {
            logger.info("Polling for records ...");
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket: scanRecords.buckets()) {
                for (ScanRecord record: scanRecords.records(bucket)) {
                    InternalRow row = record.getRow();
                    OrderPaymentInfoPartial orderPaymentInfoPartial
                            = new OrderPaymentInfoPartial(row.getString(0).toString(), row.getDouble(1), row.getString(2).toString());
                    logger.info("Bucket: {} - {}", bucket, orderPaymentInfoPartial);
                }
            }
        }
    }
}

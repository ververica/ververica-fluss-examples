package com.ververica.lookup;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.lookup.LookupResult;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.ververica.models.FoodDeliveryDomain;
import com.ververica.utils.AppUtils;
import com.ververica.writer.FoodDeliveryWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;

public class CourierLookup {
    private static final Logger logger = LoggerFactory.getLogger(CourierLookup.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("Starting {}", FoodDeliveryWriter.class.getSimpleName());
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), AppUtils.BOOTSTRAP_SERVERS);
        Connection connection = ConnectionFactory.createConnection(conf);

        Table courierLocationUpdateTable = connection.getTable(FoodDeliveryDomain.CourierLocationUpdate.tablePath());

        Lookuper lookuper = courierLocationUpdateTable.newLookup().createLookuper();

        LookupResult lookupResult = lookuper.lookup(GenericRow.of(BinaryString.fromString("1100"))).get();
        lookupResult.getRowList().forEach(row -> {
            logger.info("Lookup result: {}", row.toString());
            logger.info("{}", new FoodDeliveryDomain.CourierLocationUpdate(
                    row.getString(0).toString(),
                    row.getString(1).toString(),
                    row.getDouble(2),
                    row.getDouble(3),
                    row.getDouble(4),
                    row.getBoolean(5),
                    LocalDateTime.parse(row.getTimestampNtz(6, 6).toString(), formatter)
            ));
            logger.info("============================");
        });
    }
}

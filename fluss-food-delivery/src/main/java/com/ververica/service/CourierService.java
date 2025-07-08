package com.ververica.service;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Service
public class CourierService {
    private static final Logger logger = LoggerFactory.getLogger(CourierService.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private final Connection connection;
    private final Table courierLocationUpdateTable;
    private final Lookuper lookuper;

    public CourierService() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), AppUtils.BOOTSTRAP_SERVERS);
        this.connection = ConnectionFactory.createConnection(conf);
        this.courierLocationUpdateTable = connection.getTable(FoodDeliveryDomain.CourierLocationUpdate.tablePath());
        this.lookuper = courierLocationUpdateTable.newLookup().createLookuper();
    }

    public Optional<FoodDeliveryDomain.CourierLocationUpdate> getCourierById(String courierId) {
        try {
            LookupResult lookupResult = lookuper.lookup(GenericRow.of(BinaryString.fromString(courierId))).get();
            if (lookupResult.getRowList().isEmpty()) {
                return Optional.empty();
            }

            var row = lookupResult.getRowList().get(0);
            return Optional.of(new FoodDeliveryDomain.CourierLocationUpdate(
                    row.getString(0).toString(),
                    row.getString(1).toString(),
                    row.getDouble(2),
                    row.getDouble(3),
                    row.getDouble(4),
                    row.getBoolean(5),
                    LocalDateTime.parse(row.getTimestampNtz(6, 6).toString(), formatter)
            ));
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error looking up courier with ID {}: {}", courierId, e.getMessage());
            return Optional.empty();
        }
    }
}

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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@SpringBootApplication
public class FoodDeliveryScanner {
    private static final Logger logger = LoggerFactory.getLogger(FoodDeliveryScanner.class);
    private static final Sinks.Many<OrderPaymentInfoPartial> sink = Sinks.many().multicast().onBackpressureBuffer();
    private static final Flux<OrderPaymentInfoPartial> flux = sink.asFlux();

    public record OrderPaymentInfoPartial(String orderId, double totalAmount, String paymentMethod) {}

    public static void main(String[] args) {
        logger.info("Starting {}", FoodDeliveryScanner.class.getSimpleName());
        SpringApplication.run(FoodDeliveryScanner.class, args);
    }

    @Bean
    public RouterFunction<ServerResponse> staticResourceRouter() {
        return RouterFunctions.resources("/**", new org.springframework.core.io.ClassPathResource("static/"));
    }

    @Bean
    public void startScanner() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.execute(() -> {
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
                        sink.tryEmitNext(orderPaymentInfoPartial);
                    }
                }
            }
        });
    }

    @RestController
    public static class OrderPaymentController {
        @GetMapping(path = "/api/payments", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
        public Flux<OrderPaymentInfoPartial> getPaymentStream() {
            return flux;
        }
    }
}

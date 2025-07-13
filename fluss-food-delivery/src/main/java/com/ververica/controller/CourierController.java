package com.ververica.controller;

import com.ververica.models.FoodDeliveryDomain;
import com.ververica.service.CourierService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Controller
public class CourierController {

    private final CourierService courierService;
    private final Map<String, Sinks.Many<ServerSentEvent<FoodDeliveryDomain.CourierLocationUpdate>>> courierSinks = new ConcurrentHashMap<>();
    private final Map<String, Flux<ServerSentEvent<FoodDeliveryDomain.CourierLocationUpdate>>> courierFluxes = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @Autowired
    public CourierController(CourierService courierService) {
        this.courierService = courierService;
        // Schedule a task to update courier information every 2 seconds
        scheduler.scheduleAtFixedRate(this::updateAllCouriers, 0, 2, TimeUnit.SECONDS);
    }

    private void updateAllCouriers() {
        courierSinks.forEach((courierId, sink) -> {
            courierService.getCourierById(courierId).ifPresent(courier -> {
                ServerSentEvent<FoodDeliveryDomain.CourierLocationUpdate> event = ServerSentEvent.<FoodDeliveryDomain.CourierLocationUpdate>builder()
                        .id(String.valueOf(System.currentTimeMillis()))
                        .event("courier-update")
                        .data(courier)
                        .build();
                sink.tryEmitNext(event);
            });
        });
    }

    @GetMapping("/courier/{id}")
    public String getCourierPage(@PathVariable("id") String courierId, Model model) {
        var courierOpt = courierService.getCourierById(courierId);

        if (courierOpt.isPresent()) {
            model.addAttribute("courier", courierOpt.get());
            return "courier";
        } else {
            model.addAttribute("error", "Courier not found with ID: " + courierId);
            return "error";
        }
    }

    @GetMapping("/api/courier/{id}")
    @ResponseBody
    public ResponseEntity<FoodDeliveryDomain.CourierLocationUpdate> getCourierApi(@PathVariable("id") String courierId) {
        return courierService.getCourierById(courierId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping(path = "/api/courier/{id}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @ResponseBody
    public Flux<ServerSentEvent<FoodDeliveryDomain.CourierLocationUpdate>> streamCourierUpdates(@PathVariable("id") String courierId) {
        // Create a new sink and flux for this courier if they don't exist
        if (!courierFluxes.containsKey(courierId)) {
            Sinks.Many<ServerSentEvent<FoodDeliveryDomain.CourierLocationUpdate>> sink = 
                Sinks.many().multicast().onBackpressureBuffer();
            Flux<ServerSentEvent<FoodDeliveryDomain.CourierLocationUpdate>> flux = sink.asFlux();

            courierSinks.put(courierId, sink);
            courierFluxes.put(courierId, flux);

            // Emit initial event
            courierService.getCourierById(courierId).ifPresent(courier -> {
                ServerSentEvent<FoodDeliveryDomain.CourierLocationUpdate> event = 
                    ServerSentEvent.<FoodDeliveryDomain.CourierLocationUpdate>builder()
                        .id("0")
                        .event("courier-update")
                        .data(courier)
                        .build();
                sink.tryEmitNext(event);
            });
        }

        return courierFluxes.get(courierId);
    }
}

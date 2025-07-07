package com.ververica.controller;

import com.ververica.models.FoodDeliveryDomain;
import com.ververica.service.CourierService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class CourierController {

    private final CourierService courierService;

    @Autowired
    public CourierController(CourierService courierService) {
        this.courierService = courierService;
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
}
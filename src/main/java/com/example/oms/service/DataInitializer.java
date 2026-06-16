package com.example.oms.service;

import com.example.oms.repository.OrderRepository;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class DataInitializer implements ApplicationRunner {

    private final OrderRepository repo;

    public DataInitializer(OrderRepository repo) {
        this.repo = repo;
    }

    @Override
    public void run(ApplicationArguments args) {
        repo.deleteAll();
    }
}

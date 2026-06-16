package com.example.oms.service;

import com.example.oms.domain.OrderDetail;
import com.example.oms.domain.OrderStatus;
import com.example.oms.repository.OrderRepository;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Component
public class DataInitializer implements ApplicationRunner {

    private static final String[] SKUS    = {"SHOE-001", "SHIRT-002", "PANTS-003", "HAT-004", "JACKET-005",
                                              "SOCK-006", "BELT-007", "BAG-008",   "WATCH-009","GLOVE-010"};
    private static final int[]    PRICES  = {9999, 4999, 7999, 2999, 14999, 999, 3499, 12999, 24999, 1999};
    private static final OrderStatus[] STATUSES = {
            OrderStatus.NEW, OrderStatus.NEW,
            OrderStatus.RESERVED, OrderStatus.RESERVED,
            OrderStatus.PAID, OrderStatus.PAID,
            OrderStatus.SHIPPED, OrderStatus.SHIPPED, OrderStatus.SHIPPED,
            OrderStatus.CANCELLED
    };

    private final OrderRepository repo;

    public DataInitializer(OrderRepository repo) {
        this.repo = repo;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (repo.count() > 0) return;

        Random rng = new Random(42);
        List<OrderDetail> orders = new ArrayList<>(100);

        for (int i = 0; i < 100; i++) {
            String orderId    = UUID.randomUUID().toString();
            String customerId = String.format("CUST-%03d", (i % 20) + 1);
            OrderStatus status = STATUSES[i % STATUSES.length];
            Instant created   = Instant.now().minus(rng.nextInt(30), ChronoUnit.DAYS)
                                             .minus(rng.nextInt(1440), ChronoUnit.MINUTES);

            int itemCount   = rng.nextInt(4) + 1;
            long totalCents = 0;
            StringBuilder summary = new StringBuilder();
            for (int j = 0; j < itemCount; j++) {
                int idx   = rng.nextInt(SKUS.length);
                int qty   = rng.nextInt(3) + 1;
                long line = (long) qty * PRICES[idx];
                totalCents += line;
                if (j > 0) summary.append(", ");
                summary.append(SKUS[idx]).append(" x").append(qty);
            }

            OrderDetail o = new OrderDetail(orderId, customerId, status, totalCents,
                                            summary.toString(), itemCount, created);

            // Back-fill topic timestamps to reflect the pipeline stage reached
            if (status != OrderStatus.NEW) {
                o.setOrderEventAt(created.plusSeconds(1));
            }
            if (status == OrderStatus.RESERVED || status == OrderStatus.PAID
                    || status == OrderStatus.SHIPPED) {
                o.setInventoryCmdAt(created.plusSeconds(2));
            }
            if (status == OrderStatus.PAID || status == OrderStatus.SHIPPED) {
                o.setPaymentCmdAt(created.plusSeconds(3));
            }
            o.setUpdatedAt(created.plusSeconds(status.ordinal()));

            orders.add(o);
        }

        repo.saveAll(orders);
    }
}

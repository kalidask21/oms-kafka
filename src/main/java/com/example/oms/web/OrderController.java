package com.example.oms.web;

import com.example.oms.domain.OrderDetail;
import com.example.oms.domain.OrderState;
import com.example.oms.repository.OrderRepository;
import com.example.oms.service.OrderService;
import com.example.oms.streams.OrderStateTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final String CSV_HEADER =
            "orderId,customerId,status,totalCents,itemCount,itemsSummary," +
            "createdAt,updatedAt,orderEventAt,inventoryCmdAt,paymentCmdAt\n";

    private final OrderService orders;
    private final StreamsBuilderFactoryBean streams;
    private final OrderRepository repo;

    public OrderController(OrderService orders, StreamsBuilderFactoryBean streams, OrderRepository repo) {
        this.orders = orders;
        this.streams = streams;
        this.repo = repo;
    }

    @PostMapping
    public Map<String, String> place(@RequestBody PlaceOrderRequest req) {
        String id = orders.placeOrder(req.customerId(), req.items());
        return Map.of("orderId", id);
    }

    /** Interactive query: read the latest OrderState straight from the Streams state store. */
    @GetMapping("/{id}")
    public ResponseEntity<OrderState> get(@PathVariable String id) {
        KafkaStreams ks = streams.getKafkaStreams();
        if (ks == null || !ks.state().isRunningOrRebalancing()) {
            return ResponseEntity.status(503).build();
        }
        ReadOnlyKeyValueStore<String, OrderState> store = ks.store(
                StoreQueryParameters.fromNameAndType(
                        OrderStateTopology.STORE, QueryableStoreTypes.keyValueStore()));
        OrderState state = store.get(id);
        return state == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(state);
    }

    /** All orders from H2 as CSV — one row per order, newest first by createdAt. */
    @GetMapping(value = "/export", produces = MediaType.TEXT_PLAIN_VALUE)
    public String export() {
        List<OrderDetail> all = repo.findAll();
        all.sort((a, b) -> b.getCreatedAt().compareTo(a.getCreatedAt()));

        StringBuilder sb = new StringBuilder(CSV_HEADER);
        for (OrderDetail o : all) {
            sb.append(csv(o.getOrderId()))       .append(',')
              .append(csv(o.getCustomerId()))     .append(',')
              .append(o.getStatus())              .append(',')
              .append(o.getTotalCents())          .append(',')
              .append(o.getItemCount())           .append(',')
              .append(csv(o.getItemsSummary()))   .append(',')
              .append(o.getCreatedAt())           .append(',')
              .append(o.getUpdatedAt())           .append(',')
              .append(o.getOrderEventAt())        .append(',')
              .append(o.getInventoryCmdAt())      .append(',')
              .append(o.getPaymentCmdAt())        .append('\n');
        }
        return sb.toString();
    }

    /** Wraps a field in double-quotes and escapes any internal double-quotes. */
    private static String csv(String value) {
        if (value == null) return "";
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}

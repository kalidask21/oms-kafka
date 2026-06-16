package com.example.oms.web;

import com.example.oms.domain.OrderState;
import com.example.oms.service.OrderService;
import com.example.oms.streams.OrderStateTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orders;
    private final StreamsBuilderFactoryBean streams;

    public OrderController(OrderService orders, StreamsBuilderFactoryBean streams) {
        this.orders = orders;
        this.streams = streams;
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
}

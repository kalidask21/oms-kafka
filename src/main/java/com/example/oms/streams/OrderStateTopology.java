package com.example.oms.streams;

import com.example.oms.domain.OrderEvent;
import com.example.oms.domain.OrderState;
import com.example.oms.domain.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * The stream-table duality made concrete:
 *
 *   order.events (KStream of events)
 *      -> groupByKey (by orderId)
 *      -> aggregate  (fold each event into OrderState)
 *      => KTable<orderId, OrderState>     <-- the "current state" table
 *
 * The table's changelog is itself a stream, republished to the compacted
 * order.state topic. The materialized "order-state-store" is queryable directly
 * (see OrderController) as a low-latency read model.
 */
@Configuration
@EnableKafkaStreams
public class OrderStateTopology {

    public static final String STORE = "order-state-store";

    @Bean
    public KStream<String, OrderEvent> buildPipeline(StreamsBuilder builder) {
        JsonSerde<OrderEvent> eventSerde = new JsonSerde<>(OrderEvent.class);
        JsonSerde<OrderState> stateSerde = new JsonSerde<>(OrderState.class);
        eventSerde.deserializer().addTrustedPackages("com.example.oms.domain");
        stateSerde.deserializer().addTrustedPackages("com.example.oms.domain");

        KStream<String, OrderEvent> events =
                builder.stream(Topics.ORDER_EVENTS, Consumed.with(Serdes.String(), eventSerde));

        KTable<String, OrderState> orderState = events
                .groupByKey(Grouped.with(Serdes.String(), eventSerde))
                .aggregate(
                        OrderState::empty,
                        (orderId, event, state) -> state.apply(event),
                        Materialized.<String, OrderState, KeyValueStore<Bytes, byte[]>>as(STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(stateSerde));

        orderState.toStream().to(Topics.ORDER_STATE, Produced.with(Serdes.String(), stateSerde));
        return events;
    }
}

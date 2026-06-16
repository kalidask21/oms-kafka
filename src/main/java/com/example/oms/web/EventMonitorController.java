package com.example.oms.web;

import com.example.oms.service.EventLogStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/events")
public class EventMonitorController {

    private final EventLogStore store;

    public EventMonitorController(EventLogStore store) {
        this.store = store;
    }

    /** Last 100 messages sent to Kafka (newest first), captured at producer ack. */
    @GetMapping("/produced")
    public List<EventEntry> produced() {
        return store.getProduced();
    }

    /** Last 100 messages received from Kafka (newest first), captured before deserialization. */
    @GetMapping("/consumed")
    public List<EventEntry> consumed() {
        return store.getConsumed();
    }
}

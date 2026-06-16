package com.example.oms.service;

import com.example.oms.web.EventEntry;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@Service
public class EventLogStore {

    private static final int MAX = 100;

    private final ConcurrentLinkedDeque<EventEntry> produced = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<EventEntry> consumed = new ConcurrentLinkedDeque<>();

    public void addProduced(EventEntry entry) {
        produced.addFirst(entry);
        if (produced.size() > MAX) produced.pollLast();
    }

    public void addConsumed(EventEntry entry) {
        consumed.addFirst(entry);
        if (consumed.size() > MAX) consumed.pollLast();
    }

    public List<EventEntry> getProduced() {
        return List.copyOf(produced);
    }

    public List<EventEntry> getConsumed() {
        return List.copyOf(consumed);
    }
}

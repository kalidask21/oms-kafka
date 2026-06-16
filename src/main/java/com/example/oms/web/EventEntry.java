package com.example.oms.web;

import java.time.Instant;

public record EventEntry(String topic, String key, String type, String payload, Instant timestamp) {}

package com.example.oms.service;

import com.example.oms.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Consumes payment commands. Demo rule: charges over $10,000 are declined.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private static final long LIMIT_CENTS = 1_000_000L;
    private final KafkaTemplate<String, Object> kafka;

    public PaymentService(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 1000, multiplier = 2.0))
    @KafkaListener(topics = Topics.PAYMENT_COMMANDS, groupId = "payment-service")
    public void onCommand(PaymentCommand cmd) {
        if (cmd.action() == PaymentCommand.Action.REFUND) {
            log.info("Refunding {} for {} (compensation)", cmd.amountCents(), cmd.orderId());
            return;
        }
        if (cmd.amountCents() < LIMIT_CENTS) {
            kafka.send(Topics.ORDER_EVENTS, cmd.orderId(),
                    new PaymentCompleted(cmd.orderId(), Instant.now()));
        } else {
            kafka.send(Topics.ORDER_EVENTS, cmd.orderId(),
                    new PaymentFailed(cmd.orderId(), "amount exceeds limit", Instant.now()));
        }
    }

    @DltHandler
    public void dlt(PaymentCommand cmd) {
        log.error("Payment command exhausted retries, sent to DLT: {}", cmd);
    }
}

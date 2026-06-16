package com.example.oms.domain;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "customer_orders")
public class OrderDetail {

    @Id
    @Column(length = 36)
    private String orderId;

    @Column(nullable = false)
    private String customerId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    private long totalCents;

    /** Serialized as "SKU x QTY, ..." for readability in CSV. */
    @Column(length = 512)
    private String itemsSummary;

    private int itemCount;

    private Instant createdAt;
    private Instant updatedAt;

    /** Timestamps recording when each Kafka topic last touched this order. */
    private Instant orderEventAt;
    private Instant inventoryCmdAt;
    private Instant paymentCmdAt;

    public OrderDetail() {}

    public OrderDetail(String orderId, String customerId, OrderStatus status,
                       long totalCents, String itemsSummary, int itemCount, Instant createdAt) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.status = status;
        this.totalCents = totalCents;
        this.itemsSummary = itemsSummary;
        this.itemCount = itemCount;
        this.createdAt = createdAt;
        this.updatedAt = createdAt;
    }

    public String getOrderId()         { return orderId; }
    public String getCustomerId()      { return customerId; }
    public OrderStatus getStatus()     { return status; }
    public long getTotalCents()        { return totalCents; }
    public String getItemsSummary()    { return itemsSummary; }
    public int getItemCount()          { return itemCount; }
    public Instant getCreatedAt()      { return createdAt; }
    public Instant getUpdatedAt()      { return updatedAt; }
    public Instant getOrderEventAt()   { return orderEventAt; }
    public Instant getInventoryCmdAt() { return inventoryCmdAt; }
    public Instant getPaymentCmdAt()   { return paymentCmdAt; }

    public void setStatus(OrderStatus status)          { this.status = status; }
    public void setTotalCents(long totalCents)          { this.totalCents = totalCents; }
    public void setItemsSummary(String itemsSummary)   { this.itemsSummary = itemsSummary; }
    public void setItemCount(int itemCount)            { this.itemCount = itemCount; }
    public void setUpdatedAt(Instant updatedAt)        { this.updatedAt = updatedAt; }
    public void setOrderEventAt(Instant orderEventAt)  { this.orderEventAt = orderEventAt; }
    public void setInventoryCmdAt(Instant t)           { this.inventoryCmdAt = t; }
    public void setPaymentCmdAt(Instant t)             { this.paymentCmdAt = t; }
}

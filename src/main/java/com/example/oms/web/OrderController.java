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

    private static final Map<String, String> STATUS_COLORS = Map.of(
            "NEW",      "#3b82f6",
            "RESERVED", "#f59e0b",
            "PAID",     "#8b5cf6",
            "SHIPPED",  "#10b981",
            "CANCELLED","#ef4444"
    );

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

    /** All orders from H2 rendered as an HTML table, newest first by createdAt. */
    @GetMapping(value = "/export", produces = MediaType.TEXT_HTML_VALUE)
    public String export() {
        List<OrderDetail> all = repo.findAll();
        all.sort((a, b) -> b.getCreatedAt().compareTo(a.getCreatedAt()));

        StringBuilder sb = new StringBuilder("""
                <!doctype html><html lang="en"><head><meta charset="UTF-8">
                <title>OMS Orders</title>
                <style>
                  * { box-sizing: border-box; margin: 0; padding: 0; }
                  body { font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; padding: 24px; }
                  h1 { font-size: 1.4rem; font-weight: 600; margin-bottom: 16px; color: #f8fafc; }
                  .meta { font-size: .8rem; color: #94a3b8; margin-bottom: 20px; }
                  .wrap { overflow-x: auto; border-radius: 10px; border: 1px solid #1e293b; }
                  table { width: 100%; border-collapse: collapse; font-size: .78rem; }
                  thead tr { background: #1e293b; }
                  th { padding: 10px 14px; text-align: left; color: #94a3b8; font-weight: 600;
                       text-transform: uppercase; letter-spacing: .05em; white-space: nowrap; }
                  tbody tr { border-top: 1px solid #1e293b; }
                  tbody tr:hover { background: #1e293b88; }
                  td { padding: 9px 14px; vertical-align: middle; white-space: nowrap; }
                  .badge { display: inline-block; padding: 2px 10px; border-radius: 999px;
                           font-size: .7rem; font-weight: 700; color: #fff; }
                  .tick { color: #10b981; font-weight: 700; }
                  .dash { color: #475569; }
                  .cents { color: #a5f3fc; font-variant-numeric: tabular-nums; }
                  .ts { color: #cbd5e1; font-size: .72rem; }
                  .id { font-family: monospace; font-size: .72rem; color: #7dd3fc; }
                </style></head><body>
                """);

        sb.append("<h1>Order Management — H2 Snapshot</h1>");
        sb.append("<p class='meta'>").append(all.size()).append(" orders &nbsp;·&nbsp; newest first</p>");
        sb.append("<div class='wrap'><table><thead><tr>");
        for (String h : new String[]{"#","Order ID","Customer","Status","Total ($)","Items","Created",
                                     "order.events","inventory.commands","payments.commands","Updated"}) {
            sb.append("<th>").append(h).append("</th>");
        }
        sb.append("</tr></thead><tbody>");

        int row = 1;
        for (OrderDetail o : all) {
            String status = o.getStatus().name();
            String color  = STATUS_COLORS.getOrDefault(status, "#64748b");
            sb.append("<tr>")
              .append(td(String.valueOf(row++)))
              .append(td("<span class='id'>" + esc(o.getOrderId()) + "</span>"))
              .append(td(esc(o.getCustomerId())))
              .append(td("<span class='badge' style='background:" + color + "'>" + status + "</span>"))
              .append(td("<span class='cents'>$" + String.format("%.2f", o.getTotalCents() / 100.0) + "</span>"))
              .append(td(o.getItemCount() + " — " + esc(o.getItemsSummary())))
              .append(tdTs(o.getCreatedAt()))
              .append(tdTs(o.getOrderEventAt()))
              .append(tdTs(o.getInventoryCmdAt()))
              .append(tdTs(o.getPaymentCmdAt()))
              .append(tdTs(o.getUpdatedAt()))
              .append("</tr>");
        }

        sb.append("</tbody></table></div></body></html>");
        return sb.toString();
    }

    private static String td(String content) {
        return "<td>" + content + "</td>";
    }

    private static String tdTs(java.time.Instant ts) {
        if (ts == null) return "<td><span class='dash'>—</span></td>";
        String s = ts.toString().replace("T", " ").replace("Z", "");
        return "<td><span class='ts'>" + s + "</span></td>";
    }

    private static String esc(String v) {
        if (v == null) return "";
        return v.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}

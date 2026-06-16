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

    /** All orders from H2 rendered as an HTML table, newest first by createdAt.
     *  Optional ?search= filters by order ID or item name (contains, case-insensitive). */
    @GetMapping(value = "/export", produces = MediaType.TEXT_HTML_VALUE)
    public String export(@RequestParam(required = false, defaultValue = "") String search) {
        List<OrderDetail> all = repo.findAll();
        all.sort((a, b) -> b.getCreatedAt().compareTo(a.getCreatedAt()));

        String q = search.trim().toLowerCase();
        List<OrderDetail> rows = q.isEmpty() ? all : all.stream()
                .filter(o -> o.getOrderId().toLowerCase().contains(q)
                          || (o.getItemsSummary() != null && o.getItemsSummary().toLowerCase().contains(q)))
                .toList();

        String safeQ = esc(search);
        StringBuilder sb = new StringBuilder("""
                <!doctype html><html lang="en"><head><meta charset="UTF-8">
                <title>OMS Service</title>
                <style>
                  * { box-sizing: border-box; margin: 0; padding: 0; }
                  body { font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; padding: 24px; }
                  h1 { font-size: 1.5rem; font-weight: 700; color: #f8fafc; }
                  .header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 20px; }
                  /* Place Order button */
                  .btn-order { background: #10b981; border: none; border-radius: 7px; color: #fff;
                               padding: 8px 18px; font-size: .85rem; font-weight: 600; cursor: pointer; }
                  .btn-order:hover { background: #059669; }
                  /* Order form panel */
                  .order-panel { background: #1e293b; border: 1px solid #334155; border-radius: 10px;
                                 padding: 20px 24px; margin-bottom: 22px; display: none; }
                  .order-panel.open { display: block; }
                  .order-panel h2 { font-size: 1rem; font-weight: 600; margin-bottom: 14px; color: #f1f5f9; }
                  .form-row { display: flex; gap: 10px; align-items: center; margin-bottom: 10px; flex-wrap: wrap; }
                  .form-row label { font-size: .78rem; color: #94a3b8; width: 110px; flex-shrink: 0; }
                  .form-row input { background: #0f172a; border: 1px solid #334155; border-radius: 6px;
                                    color: #e2e8f0; padding: 6px 10px; font-size: .82rem; outline: none; }
                  .form-row input:focus { border-color: #3b82f6; }
                  .inp-cust { width: 200px; }
                  .inp-sku  { width: 160px; }
                  .inp-num  { width: 80px; }
                  .items-table { width: 100%; border-collapse: collapse; margin-bottom: 12px; }
                  .items-table th { font-size: .72rem; color: #64748b; text-transform: uppercase;
                                    letter-spacing: .04em; padding: 4px 8px; text-align: left; }
                  .items-table td { padding: 5px 8px; }
                  .items-table input { background: #0f172a; border: 1px solid #334155; border-radius: 5px;
                                       color: #e2e8f0; padding: 5px 8px; font-size: .8rem; width: 100%; outline: none; }
                  .items-table input:focus { border-color: #3b82f6; }
                  .btn-sm { background: #334155; border: none; border-radius: 5px; color: #e2e8f0;
                            padding: 4px 10px; font-size: .75rem; cursor: pointer; }
                  .btn-sm:hover { background: #475569; }
                  .btn-primary { background: #3b82f6; border: none; border-radius: 6px; color: #fff;
                                 padding: 7px 16px; font-size: .82rem; cursor: pointer; font-weight: 600; }
                  .btn-primary:hover { background: #2563eb; }
                  .note { background: #451a03; border: 1px solid #92400e; border-radius: 6px;
                          color: #fcd34d; font-size: .76rem; padding: 8px 12px; margin: 12px 0; }
                  .note b { color: #fbbf24; }
                  .result-ok  { color: #34d399; font-size: .82rem; margin-top: 10px; }
                  .result-err { color: #f87171; font-size: .82rem; margin-top: 10px; }
                  /* Search toolbar */
                  .toolbar { display: flex; align-items: center; gap: 10px; margin-bottom: 18px; }
                  .toolbar form { display: flex; gap: 8px; }
                  .toolbar input { background: #1e293b; border: 1px solid #334155; border-radius: 6px;
                                   color: #e2e8f0; padding: 6px 12px; font-size: .82rem; width: 280px; outline: none; }
                  .toolbar input:focus { border-color: #3b82f6; }
                  .toolbar button { background: #3b82f6; border: none; border-radius: 6px; color: #fff;
                                    padding: 6px 14px; font-size: .82rem; cursor: pointer; }
                  .toolbar button:hover { background: #2563eb; }
                  .clear { font-size: .78rem; color: #94a3b8; text-decoration: none; }
                  .clear:hover { color: #e2e8f0; }
                  .meta { font-size: .8rem; color: #94a3b8; margin-bottom: 16px; }
                  /* Table */
                  .wrap { overflow-x: auto; border-radius: 10px; border: 1px solid #1e293b; }
                  table.orders { width: 100%; border-collapse: collapse; font-size: .78rem; }
                  table.orders thead tr { background: #1e293b; }
                  table.orders th { padding: 10px 14px; text-align: left; color: #94a3b8; font-weight: 600;
                       text-transform: uppercase; letter-spacing: .05em; white-space: nowrap; }
                  table.orders tbody tr { border-top: 1px solid #1e293b; }
                  table.orders tbody tr:hover { background: #1e293b88; }
                  table.orders td { padding: 9px 14px; vertical-align: middle; white-space: nowrap; }
                  .badge { display: inline-block; padding: 2px 10px; border-radius: 999px;
                           font-size: .7rem; font-weight: 700; color: #fff; }
                  .dash { color: #475569; }
                  .cents { color: #a5f3fc; font-variant-numeric: tabular-nums; }
                  .ts { color: #cbd5e1; font-size: .72rem; }
                  .id { font-family: monospace; font-size: .72rem; color: #7dd3fc; }
                  .hl { background: #854d0e; border-radius: 2px; padding: 0 2px; }
                  .empty { padding: 40px; text-align: center; color: #475569; }
                </style></head><body>
                """);

        // ── Header ──────────────────────────────────────────────────────
        sb.append("""
                <div class="header">
                  <h1>OMS Service</h1>
                  <button class="btn-order" onclick="document.getElementById('panel').classList.toggle('open')">
                    + Place Order
                  </button>
                </div>
                """);

        // ── Place Order panel ────────────────────────────────────────────
        sb.append("""
                <div class="order-panel" id="panel">
                  <h2>New Order</h2>
                  <div class="form-row">
                    <label>Customer Name</label>
                    <input id="custId" class="inp-cust" placeholder="e.g. CUST-001">
                  </div>
                  <table class="items-table">
                    <thead><tr>
                      <th>SKU</th><th>Qty</th><th>Unit Price ($)</th><th></th>
                    </tr></thead>
                    <tbody id="itemRows">
                      <tr>
                        <td><input class="sku" placeholder="e.g. SHOE-001"></td>
                        <td><input class="qty inp-num" type="number" min="1" value="1"></td>
                        <td><input class="price inp-num" type="number" min="0.01" step="0.01" placeholder="99.99"></td>
                        <td></td>
                      </tr>
                    </tbody>
                  </table>
                  <div style="display:flex;gap:8px;align-items:center;margin-bottom:12px">
                    <button class="btn-sm" onclick="addItem()">+ Add Item</button>
                    <button class="btn-primary" onclick="submitOrder()">Submit Order</button>
                  </div>
                  <div class="note">
                    ⚠️ <b>Payment failure rule:</b> orders with a total value <b>≥&nbsp;$10,000</b>
                    are declined by PaymentService — the order will be set to <b>CANCELLED</b>.
                    Keep the sum of (unit price × qty) across all items below $10,000 to reach SHIPPED status.
                  </div>
                  <div id="orderResult"></div>
                </div>
                <script>
                function addItem() {
                  const tbody = document.getElementById('itemRows');
                  const tr = document.createElement('tr');
                  tr.innerHTML = `
                    <td><input class="sku" placeholder="e.g. SHIRT-002"></td>
                    <td><input class="qty inp-num" type="number" min="1" value="1"></td>
                    <td><input class="price inp-num" type="number" min="0.01" step="0.01" placeholder="49.99"></td>
                    <td><button class="btn-sm" onclick="this.closest('tr').remove()">✕</button></td>`;
                  tbody.appendChild(tr);
                }
                async function submitOrder() {
                  const customerId = document.getElementById('custId').value.trim();
                  if (!customerId) { alert('Customer Name is required'); return; }
                  const skus   = [...document.querySelectorAll('.sku')];
                  const qtys   = [...document.querySelectorAll('.qty')];
                  const prices = [...document.querySelectorAll('.price')];
                  const items  = skus.map((s,i) => ({
                    sku: s.value.trim(),
                    qty: parseInt(qtys[i].value) || 1,
                    unitPriceCents: Math.round((parseFloat(prices[i].value) || 0) * 100)
                  })).filter(it => it.sku);
                  if (!items.length) { alert('Add at least one item with a SKU'); return; }
                  const res = document.getElementById('orderResult');
                  try {
                    const r = await fetch('/orders', {
                      method:'POST',
                      headers:{'Content-Type':'application/json'},
                      body: JSON.stringify({customerId, items})
                    });
                    const d = await r.json();
                    res.innerHTML = `<p class="result-ok">✓ Order placed — ID: <b>${d.orderId}</b>. Refreshing…</p>`;
                    setTimeout(() => location.reload(), 2500);
                  } catch(e) {
                    res.innerHTML = `<p class="result-err">✗ ${e.message}</p>`;
                  }
                }
                </script>
                """);

        // ── Search bar ───────────────────────────────────────────────────
        sb.append("<div class='toolbar'><form method='get' action='/orders/export'>")
          .append("<input name='search' placeholder='Search by order ID or item name…' value='").append(safeQ).append("'>")
          .append("<button type='submit'>Search</button></form>");
        if (!q.isEmpty()) {
            sb.append("<a class='clear' href='/orders/export'>✕ Clear</a>");
        }
        sb.append("</div>");

        sb.append("<p class='meta'>").append(rows.size()).append(" of ").append(all.size())
          .append(" orders").append(q.isEmpty() ? "" : " matching <b>" + safeQ + "</b>")
          .append(" &nbsp;·&nbsp; newest first</p>");

        // ── Orders table ─────────────────────────────────────────────────
        sb.append("<div class='wrap'><table class='orders'><thead><tr>");
        for (String h : new String[]{"#","Order ID","Customer","Status","Total ($)","Items","Created",
                                     "order.events","inventory.commands","payments.commands","Updated"}) {
            sb.append("<th>").append(h).append("</th>");
        }
        sb.append("</tr></thead><tbody>");

        if (rows.isEmpty()) {
            sb.append("<tr><td colspan='11' class='empty'>No orders match your search.</td></tr>");
        } else {
            int row = 1;
            for (OrderDetail o : rows) {
                String status = o.getStatus().name();
                String color  = STATUS_COLORS.getOrDefault(status, "#64748b");
                sb.append("<tr>")
                  .append(td(String.valueOf(row++)))
                  .append(td("<span class='id'>" + highlight(o.getOrderId(), q) + "</span>"))
                  .append(td(esc(o.getCustomerId())))
                  .append(td("<span class='badge' style='background:" + color + "'>" + status + "</span>"))
                  .append(td("<span class='cents'>$" + String.format("%.2f", o.getTotalCents() / 100.0) + "</span>"))
                  .append(td(o.getItemCount() + " — " + highlight(o.getItemsSummary(), q)))
                  .append(tdTs(o.getCreatedAt()))
                  .append(tdTs(o.getOrderEventAt()))
                  .append(tdTs(o.getInventoryCmdAt()))
                  .append(tdTs(o.getPaymentCmdAt()))
                  .append(tdTs(o.getUpdatedAt()))
                  .append("</tr>");
            }
        }

        sb.append("</tbody></table></div></body></html>");
        return sb.toString();
    }

    /** Highlights the search term inside a field value (case-insensitive). */
    private static String highlight(String value, String q) {
        if (value == null) return "";
        if (q.isEmpty()) return esc(value);
        String escaped = esc(value);
        String lower   = value.toLowerCase();
        int idx = lower.indexOf(q);
        if (idx == -1) return escaped;
        return esc(value.substring(0, idx))
             + "<span class='hl'>" + esc(value.substring(idx, idx + q.length())) + "</span>"
             + esc(value.substring(idx + q.length()));
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

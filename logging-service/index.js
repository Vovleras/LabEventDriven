const express = require('express');
const { Kafka } = require('kafkajs');

const SERVICE = process.env.SERVICE_NAME || 'logging-service';
const PORT = parseInt(process.env.PORT || '3009', 10);
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:29092';

const app = express();
app.use(express.json());

const kafka = new Kafka({ clientId: SERVICE, brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: `${SERVICE}-group` });

// Keep a bounded in-memory list of events for the demo UI
const MAX_EVENTS = 1000;
const events = [];

// SSE clients
const sseClients = new Set();

const topics = ['order.created','order.completed','payment.success','payment.failed','inventory.updated','shipment.sent','shipment.delivered','user.created','user.updated','product.created','product.updated'];

const pushEvent = (evt) => {
  // add to in-memory store
  events.push(evt);
  if (events.length > MAX_EVENTS) events.shift();

  // push to SSE clients
  const data = `data: ${JSON.stringify(evt)}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(data);
    } catch (e) {
      // ignore broken pipe, will be cleaned on close
    }
  }
};

const connect = async () => {
  try {
    await consumer.connect();
    console.log(`${SERVICE}: consumer connected`);
    for (const t of topics) {
      await consumer.subscribe({ topic: t, fromBeginning: false });
      console.log(`${SERVICE}: subscribed to ${t}`);
    }

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const key = message.key && message.key.toString();
        const value = message.value && message.value.toString();
        const evt = {
          timestamp: new Date().toISOString(),
          topic,
          key: key || null,
          value: value || null
        };
        console.log(`${SERVICE}: LOG ${topic} - ${key} - ${value}`);
        pushEvent(evt);
      }
    });
  } catch (err) { console.error(`${SERVICE}: kafka connect error`, err); setTimeout(connect, 5000); }
};

connect();

// Serve a simple HTML page with an EventSource to stream events
app.get('/', (req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Logging Service - Event Pipeline</title>
  <style>
    body{font-family: Arial, Helvetica, sans-serif; background:#f7f9fc; color:#111}
    header{padding:16px; background:#2b6cb0; color:#fff}
    .container{max-width:1100px;margin:18px auto}
    .event{background:#fff;border-radius:6px;padding:12px;margin:8px 0;box-shadow:0 1px 3px rgba(0,0,0,.06)}
    .meta{font-size:12px;color:#666}
    .topic{font-weight:700;color:#2b6cb0}
    #events{height:70vh;overflow:auto;padding:8px}
    .badge{display:inline-block;padding:2px 8px;border-radius:12px;background:#edf2ff;color:#2b6cb0;font-weight:600;margin-right:8px}
  </style>
</head>
<body>
  <header><div class="container"><h2>Logging Service — Event Pipeline</h2></div></header>
  <div class="container">
    <p>Live stream of events. The page updates as events arrive.</p>
    <div id="events"></div>
  </div>
  <script>
    const eventsEl = document.getElementById('events');
    const es = new EventSource('/stream');
    es.onmessage = (e) => {
      try {
        const d = JSON.parse(e.data);
        const el = document.createElement('div');
        el.className = 'event';
        el.innerHTML = '<div style="display:flex;justify-content:space-between"><div><span class="badge">' + d.topic + '</span><span class="meta">' + d.timestamp + '</span></div><div class="meta">Key: ' + (d.key || '-') + ' </div></div><pre style="white-space:pre-wrap;margin-top:8px">' + escapeHtml(d.value) + '</pre>';
        eventsEl.insertBefore(el, eventsEl.firstChild);
      } catch (err) { console.error('parse err', err); }
    };
    es.onerror = (err) => { console.error('EventSource error', err); };
    // helper
    function escapeHtml(str){ if(!str) return ''; return str.replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
    // fetch recent events on load
    fetch('/events').then(r=>r.json()).then(list=>{
      list.reverse().forEach(d=>{
        const el = document.createElement('div');
        el.className = 'event';
        el.innerHTML = '<div style="display:flex;justify-content:space-between"><div><span class="badge">' + d.topic + '</span><span class="meta">' + d.timestamp + '</span></div><div class="meta">Key: ' + (d.key || '-') + ' </div></div><pre style="white-space:pre-wrap;margin-top:8px">' + escapeHtml(d.value) + '</pre>';
        eventsEl.appendChild(el);
      })
    }).catch(()=>{});
  </script>
</body>
</html>
  `);
});

// Server-Sent Events endpoint
app.get('/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders && res.flushHeaders();

  // send a comment to keep connection alive
  res.write(':ok\n\n');
  sseClients.add(res);

  req.on('close', () => {
    sseClients.delete(res);
  });
});

// recent events JSON
app.get('/events', (req, res) => {
  res.json(events.slice(-200));
});

app.get('/health', (req, res) => res.json({ status: 'healthy', service: SERVICE }));

const server = app.listen(PORT, () => console.log(`${SERVICE} listening on ${PORT}`));

process.on('SIGTERM', async () => { try { await consumer.disconnect(); } catch (e) {} server.close(() => process.exit(0)); });

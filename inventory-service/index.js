const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const kafka = new Kafka({ clientId: 'inventory-service', brokers: [process.env.KAFKA_BROKER || 'kafka:29092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'inventory-group' });

// Simple in-memory inventory and reservations for demo
const inventory = {
  'ITEM-001': { name: 'Laptop', stock: 50 },
  'ITEM-002': { name: 'Mouse', stock: 200 },
  'ITEM-003': { name: 'Keyboard', stock: 100 }
};
const reservations = Object.create(null);

async function connectKafka() {
  const topicsToEnsure = ['order.created', 'product-events', 'inventory.updated'];

  // Helper: ensure topics exist and have leaders before connecting producer/consumer
  async function ensureTopics(topics, retries = 5, delayMs = 2000) {
    const admin = kafka.admin();
    for (let i = 0; i < retries; i++) {
      try {
        await admin.connect();
        const created = await admin.createTopics({ topics: topics.map(t => ({ topic: t, numPartitions: 1 })), waitForLeaders: true });
        await admin.disconnect();
        console.log('Inventory Service: ensureTopics result=', created, 'topics=', topics);
        return;
      } catch (err) {
        try { await admin.disconnect(); } catch (e) {}
        console.warn(`Inventory Service: ensureTopics attempt ${i + 1} failed, retrying...`, err && err.message);
        if (i < retries - 1) await new Promise(r => setTimeout(r, delayMs));
      }
    }
    throw new Error('Inventory Service: failed to ensure Kafka topics after retries');
  }

  try {
    // Make best-effort to create / wait for topic leaders so clients don't see leader-election errors
    try {
      await ensureTopics(topicsToEnsure, 6, 2500);
    } catch (err) {
      console.warn('Inventory Service: ensureTopics failed, continuing to connect — broker may still be initializing', err && err.message);
    }

    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({ topic: 'order.created', fromBeginning: false });
    await consumer.subscribe({ topic: 'product-events', fromBeginning: false });

    console.log('Inventory Service: Kafka connected');

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        let event = null;
        try { event = JSON.parse(message.value.toString()); } catch (e) { console.error('Inventory: invalid message', e); return; }

        if (topic === 'order.created' && event && event.eventType === 'ORDER_CREATED' && event.data) {
          const orderId = event.data.orderId;
          const items = Array.isArray(event.data.items) ? event.data.items : [];
          const normalized = items.map(it => ({ itemId: it.itemId || it.productId, quantity: Number(it.quantity || it.qty || 0) }));

          const available = normalized.every(it => {
            const ent = inventory[it.itemId];
            return ent && (Number(ent.stock) || 0) >= it.quantity;
          });

          const reserved = [];
          if (available) {
            normalized.forEach(it => {
              inventory[it.itemId].stock -= it.quantity;
              reserved.push({ itemId: it.itemId, quantity: it.quantity, remaining: inventory[it.itemId].stock });
            });
            reservations[orderId] = { orderId, items: reserved, available: true, timestamp: new Date().toISOString() };
          } else {
            reservations[orderId] = { orderId, items: normalized, available: false, timestamp: new Date().toISOString() };
          }

          try {
            await producer.send({ topic: 'inventory.updated', messages: [{ key: orderId, value: JSON.stringify({ eventType: available ? 'INVENTORY_RESERVED' : 'INVENTORY_INSUFFICIENT', data: { orderId, available, reservedItems: reserved, timestamp: new Date().toISOString() } }) }] });
            console.log('Inventory: published inventory.updated for', orderId);
          } catch (e) { console.error('Inventory: publish failed', e); }
        }

        if (topic === 'product-events' && event && event.eventType === 'PRODUCT_CREATED' && event.data) {
          const p = event.data;
          inventory[p.productId] = { name: p.name, stock: Number(p.quantity || p.qty || 0) };
          console.log('Inventory: added product', p.productId);
        }
      }
    });
  } catch (err) {
    console.error('Inventory Service: Kafka connection error', err);
    setTimeout(connectKafka, 5000);
  }
}

connectKafka();

app.get('/inventory', (req, res) => res.json({ inventory }));
app.get('/reservations', (req, res) => res.json({ reservations }));
app.get('/health', (req, res) => res.json({ status: 'healthy', service: 'inventory-service' }));

const PORT = Number(process.env.PORT || 3003);
app.listen(PORT, () => console.log(`inventory-service listening on ${PORT}`));

process.on('SIGTERM', async () => { try { await consumer.disconnect(); } catch {} try { await producer.disconnect(); } catch {} process.exit(0); });
          

const express = require('express');
const { Kafka } = require('kafkajs');

const SERVICE = process.env.SERVICE_NAME || 'analytics-service';
const PORT = parseInt(process.env.PORT || '3010', 10);
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:29092';

const app = express();
app.use(express.json());

const kafka = new Kafka({ clientId: SERVICE, brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: `${SERVICE}-group` });

const topics = ['order.completed','payment.success'];
let counters = { ordersCompleted: 0, paymentsSuccess: 0 };

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
        const value = message.value && message.value.toString();
        console.log(`${SERVICE}: received ${topic} - ${value}`);
        if (topic === 'order.completed') counters.ordersCompleted++;
        if (topic === 'payment.success') counters.paymentsSuccess++;
      }
    });
  } catch (err) { console.error(`${SERVICE}: kafka connect error`, err); setTimeout(connect, 5000); }
};

connect();

app.get('/', (req, res) => res.json({ service: SERVICE, metrics: counters }));
app.get('/health', (req, res) => res.json({ status: 'healthy', service: SERVICE }));

const server = app.listen(PORT, () => console.log(`${SERVICE} listening on ${PORT}`));

process.on('SIGTERM', async () => { try { await consumer.disconnect(); } catch (e) {} server.close(() => process.exit(0)); });

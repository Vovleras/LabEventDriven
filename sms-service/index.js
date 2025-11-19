const express = require('express');
const { Kafka } = require('kafkajs');

const SERVICE = process.env.SERVICE_NAME || 'sms-service';
const PORT = parseInt(process.env.PORT || '3008', 10);
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:29092';

const app = express();
app.use(express.json());

const kafka = new Kafka({ clientId: SERVICE, brokers: [KAFKA_BROKER] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: `${SERVICE}-group` });

const servicesConfig = {
  'order-service': { consumes: ['payment.success'], produces: ['order.created','order.completed'] },
  'payment-service': { consumes: ['order.created'], produces: ['payment.success','payment.failed'] },
  'shipping-service': { consumes: ['order.completed'], produces: ['shipment.sent','shipment.delivered'] },
  'email-service': { consumes: ['order.created','shipment.sent'], produces: [] },
  'sms-service': { consumes: ['order.completed','shipment.delivered'], produces: [] },
  'logging-service': { consumes: ['order.created','order.completed','payment.success','payment.failed','inventory.updated','shipment.sent','shipment.delivered','user.created','user.updated','product.created','product.updated'], produces: [] },
  'analytics-service': { consumes: ['order.completed','payment.success'], produces: [] }
};

const config = servicesConfig[SERVICE] || { consumes: [], produces: [] };

const connect = async () => {
  try {
    await producer.connect();
    console.log(`${SERVICE}: producer connected`);
    await consumer.connect();
    console.log(`${SERVICE}: consumer connected`);

    for (const t of config.consumes) {
      await consumer.subscribe({ topic: t, fromBeginning: false });
      console.log(`${SERVICE}: subscribed to ${t}`);
    }

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const key = message.key && message.key.toString();
        const value = message.value && message.value.toString();
        console.log(`${SERVICE}: received ${topic} - ${key} - ${value}`);

        // Simulate sending SMS
        if (topic === 'order.completed' || topic === 'shipment.delivered') {
          console.log(`sms-service: send sms for ${topic} -> ${key}`);
        }
      }
    });
  } catch (err) { console.error(`${SERVICE}: kafka connect error`, err); setTimeout(connect, 5000); }
};

connect();

app.get('/', (req, res) => res.json({ service: SERVICE }));
app.get('/health', (req, res) => res.json({ status: 'healthy', service: SERVICE }));

const server = app.listen(PORT, () => console.log(`${SERVICE} listening on ${PORT}`));

process.on('SIGTERM', async () => { try { await producer.disconnect(); await consumer.disconnect(); } catch (e) {} server.close(() => process.exit(0)); });

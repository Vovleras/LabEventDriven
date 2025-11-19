const express = require('express');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const SERVICE = process.env.SERVICE_NAME || 'order-service';
const PORT = parseInt(process.env.PORT || '3004', 10);
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

        try {
          const payload = value ? JSON.parse(value) : null;

          // Payment service reacts to order.created
          if (SERVICE === 'payment-service' && topic === 'order.created') {
            // simulate payment processing
            const orderId = payload && payload.data && payload.data.orderId;
            console.log('payment-service: processing payment for', orderId);
            setTimeout(async () => {
              await producer.send({
                topic: 'payment.success',
                messages: [{ key: orderId || uuidv4(), value: JSON.stringify({ eventType: 'PAYMENT_SUCCESS', data: { orderId } }) }]
              });
              console.log('payment-service: published payment.success for', orderId);
            }, 1000);
          }

          // Order service reacts to payment.success and publishes order.completed
          if (SERVICE === 'order-service' && topic === 'payment.success') {
            const orderId = payload && payload.data && payload.data.orderId;
            console.log('order-service: payment confirmed for', orderId);
            await producer.send({
              topic: 'order.completed',
              messages: [{ key: orderId || uuidv4(), value: JSON.stringify({ eventType: 'ORDER_COMPLETED', data: { orderId } }) }]
            });
            console.log('order-service: published order.completed for', orderId);
          }

          // Shipping service reacts to order.completed
          if (SERVICE === 'shipping-service' && topic === 'order.completed') {
            const orderId = payload && payload.data && payload.data.orderId;
            setTimeout(async () => {
              await producer.send({
                topic: 'shipment.sent',
                messages: [{ key: orderId || uuidv4(), value: JSON.stringify({ eventType: 'SHIPMENT_SENT', data: { orderId } }) }]
              });
              console.log('shipping-service: published shipment.sent for', orderId);
            }, 500);
          }

          // Logging service simply logs (already printing above)
          // Analytics service could aggregate (for demo we log)
        } catch (err) {
          console.error(`${SERVICE}: error handling message`, err);
        }
      }
    });
  } catch (err) {
    console.error(`${SERVICE}: kafka connect error`, err);
    setTimeout(connect, 5000);
  }
};

connect();

// helper to publish
const publish = async (topic, key, obj) => {
  try {
    await producer.send({ topic, messages: [{ key, value: JSON.stringify(obj) }] });
    console.log(`${SERVICE}: published ${topic} - ${key}`);
  } catch (err) {
    console.error(`${SERVICE}: publish error`, err);
  }
};

// Order creation endpoint (only for order-service)
app.post('/orders', async (req, res) => {
  if (SERVICE !== 'order-service') return res.status(404).json({ error: 'not supported' });
  const orderId = `ORDER-${uuidv4()}`;
  const order = { orderId, items: req.body.items || [], timestamp: new Date().toISOString() };
  await publish('order.created', orderId, { eventType: 'ORDER_CREATED', data: order });
  return res.status(201).json({ message: 'order.created emitted', order });
});

app.get('/', (req, res) => res.json({ service: SERVICE }));
app.get('/health', (req, res) => res.json({ status: 'healthy', service: SERVICE }));

const server = app.listen(PORT, () => console.log(`${SERVICE} listening on ${PORT}`));

process.on('SIGTERM', async () => {
  try { await producer.disconnect(); await consumer.disconnect(); } catch (e) {}
  server.close(() => process.exit(0));
});

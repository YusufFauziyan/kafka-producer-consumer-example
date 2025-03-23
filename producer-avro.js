const { Kafka } = require("kafkajs");
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry");
const { faker } = require("@faker-js/faker");

const kafka = new Kafka({
  clientId: "order-producer",
  brokers: ["localhost:9092"], // Sesuaikan dengan broker Kafka
});

const producer = kafka.producer();
const schemaRegistry = new SchemaRegistry({ host: "http://localhost:8085" }); // Sesuaikan dengan host Schema Registry

const run = async () => {
  await producer.connect();

  // Ambil ID schema terbaru dari Schema Registry
  const schemaId = await schemaRegistry.getLatestSchemaId("orders-schema");

  setInterval(async () => {
    try {
      // Generate data
      const message = {
        order_id: faker.string.uuid(),
        product_name: faker.commerce.productName(),
        quantity: faker.number.int({ min: 1, max: 10 }),
        price: parseFloat(faker.finance.amount(10000, 100000, 2)),
      };

      // Encode pesan dengan Avro Schema
      const encodedMessage = await schemaRegistry.encode(schemaId, message);

      await producer.send({
        topic: "orders",
        messages: [{ key: faker.string.uuid(), value: encodedMessage }],
      });

      increment++;
      console.log("Order sent:", message);
    } catch (error) {
      console.error("Error sending order:", error);
    }
  }, 10); // Jeda 3 detik
};

run().catch(console.error);

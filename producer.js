const { Kafka } = require("kafkajs");
const { v4: uuidv4 } = require("uuid");

const kafka = new Kafka({
  clientId: "order-producer",
  brokers: ["localhost:9092"], // Sesuaikan dengan broker Kafka Anda
});

const producer = kafka.producer();

const getRandomOrder = () => {
  const products = [
    { name: "Laptop", price: 1500 },
    { name: "Smartphone", price: 800 },
    { name: "Headphones", price: 200 },
    { name: "Keyboard", price: 100 },
    { name: "Mouse", price: 50 },
  ];

  const product = products[Math.floor(Math.random() * products.length)];
  return {
    order_id: uuidv4(),
    product_name: product.name,
    quantity: Math.floor(Math.random() * 5) + 1,
    price: product.price,
  };
};

const sendMessage = async () => {
  await producer.connect();
  console.log("Producer connected...");

  setInterval(async () => {
    const order = getRandomOrder();
    await producer.send({
      topic: "orders",
      messages: [{ key: order.order_id, value: JSON.stringify(order) }],
    });
    console.log(`Sent order:`, order);
  }, 3000); // Kirim setiap 3 detik
};

sendMessage().catch(console.error);

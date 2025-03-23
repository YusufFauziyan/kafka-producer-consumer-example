const { Kafka } = require("kafkajs");
const mysql = require("mysql2/promise");

// Konfigurasi Kafka
const kafka = new Kafka({
  clientId: "order-consumer-2",
  brokers: ["localhost:9092"], // Sesuaikan dengan broker Kafka Anda
});

const consumer = kafka.consumer({ groupId: "order-group" });

// Konfigurasi koneksi MySQL
const dbConfig = {
  host: "localhost", // Sesuaikan dengan konfigurasi MySQL Anda
  port: 3306,
  user: "root",
  password: "root",
  database: "order_db",
};

const run = async () => {
  try {
    // Koneksi ke Kafka
    await consumer.connect();
    await consumer.subscribe({ topic: "orders", fromBeginning: true });

    // Koneksi ke database
    const connection = await mysql.createConnection(dbConfig);
    console.log("Connected to MySQL database");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const orderData = JSON.parse(message.value.toString());
          console.log("Received order:", orderData);

          const { order_id, product_name, quantity, price } = orderData;

          // Simpan ke database
          const query = `
            INSERT INTO orders (order_id, product_name, quantity, price, created_at)
            VALUES (?, ?, ?, ?, NOW())
          `;
          await connection.execute(query, [
            order_id,
            product_name,
            quantity,
            price,
          ]);

          console.log("Order inserted into database:", order_id);
        } catch (error) {
          console.error("Error processing message:", error);
        }
      },
    });
  } catch (error) {
    console.error("Error:", error);
  }
};

run();

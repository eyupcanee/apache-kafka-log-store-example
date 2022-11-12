const { Kafka } = require("kafkajs");

const log_data = require("./system_logs.json");
createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_log_store_clientA",
      brokers: ["localhost:9092"],
    });

    const producer = kafka.producer();
    console.log("Producer'a bağlanılıyor...");
    await producer.connect();
    console.log("Producer'a bağlantı başarılı.");

    let messages = log_data.map((item) => {
      return {
        value: JSON.stringify(item),
        partition: item.type == "system" ? 0 : 1,
      };
    });

    const message_result = await producer.send({
      topic: "LogStoreTopicA",
      messages: messages,
    });

    console.log("Gonderim islemi basarılıdır", JSON.stringify(message_result));
    await producer.disconnect();
  } catch (error) {
    console.log("Bir Hata Oluştu : ", error);
  } finally {
    process.exit(0);
  }
}

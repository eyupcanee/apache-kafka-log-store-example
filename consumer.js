const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_log_store_clientA",
      brokers: ["localhost:9092"],
    });

    const consumer = kafka.consumer({
      groupId: "log_store_consumer_group",
    });
    console.log("Consumer'a bağlanılıyor...");
    await consumer.connect();
    console.log("Consumer'a bağlantı başarılı.");

    //Consumer Subscribe
    await consumer.subscribe({
      topic: "LogStoreTopicA",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async (result) => {
        console.log(
          `Gelen Mesaj ${result.message.value} : Partition : => ${result.partition}`
        );
      },
    });
  } catch (error) {
    console.log("Bir Hata Oluştu : ", error);
  }
}

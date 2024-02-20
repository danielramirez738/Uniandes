const { Kafka, Partitioners } = require("kafkajs");

async function produceMessages() {
  const kafka = new Kafka({
    clientId: "topic-producer",
    brokers: ["localhost:9092"],
  });

  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  });

  await producer.connect();
  console.log("Conectado al broker de Kafka");

  const topic = "topic-producer";

  const messages = Array(200)
    .fill()
    .map((_, i) => ({
      key: `key-${i}`,
      value: `Mensaje número ${i + 1}`,
    }));

  await Promise.all(
    messages.map(async (message) => {
      await producer.send({
        topic,
        messages: [message],
      });

      console.log(`Mensaje enviado= ${message.value}`);
    })
  );

  await producer.disconnect();
  console.log("Desconectado del broker de Kafka");
}

produceMessages().catch(console.error);

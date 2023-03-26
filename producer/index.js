import { Kafka, Partitioners } from "kafkajs";
import { v4 as UUID } from "uuid";
console.log("*** Producer starts... ***");

const kafka = new Kafka({
  clientId: "my-checking-client",
  brokers: ["localhost:9092"],
});

const consumer2 = kafka.consumer({ groupId: "kafka-checker-servers2" });

const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});
const run = async () => {
  //Producing
  await producer.connect();
  await consumer2.connect();
  //Subscribe to the topic filled with correct id's and print them to the console.
  await consumer2.subscribe({ topic: "checkedresult", fromBeginning: true });
  await consumer2.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("Henkilötunnus oikein");
      console.log({
        key: message.key.toString(),
        value: message.value.toString(),
      });
    },
  });

  setInterval(() => {
    queueMessage();
  }, 5500);
};
run().catch(console.error);

const idNumbers = [
  "311299-999X",
  "010703A999Y",
  "240588+9999",
  "NNN588+9999",
  "112233-9999",
  "300233-9999",
  "30233-9999",
];

function randomizeIntegerBetween(from, to) {
  return Math.floor(Math.random() * (to - from + 1)) + from;
}

async function queueMessage() {
  const uuidFraction = UUID().substring(0, 4);

  const success = await producer.send({
    topic: "tobechecked",
    messages: [
      {
        key: uuidFraction,
        value: Buffer.from(
          idNumbers[randomizeIntegerBetween(0, idNumbers.length - 1)]
        ),
      },
    ],
  });

  if (success) {
    console.log(`Message ${uuidFraction} succesfully added to the stream`);
  } else {
    console.log("Problem writing to stream..");
  }
}

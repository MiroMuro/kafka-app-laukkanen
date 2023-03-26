import { Kafka, Partitioners } from "kafkajs";
console.log("*** Consumer starts***");

const kafka = new Kafka({
  clientId: "checker-server",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "kafka-checker-servers1" });

//Check that the length is correct and the first character is a number
const checkId = (id) => {
  if (id.length != 11 || isNaN(id[0]) === true) {
    return false;
  } else {
    return true;
  }
};
const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "tobechecked", fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      //If the id is in correct form create a new producier and
      //send the key of the message and the id into the "checkedresult" topic.
      if (checkId(message.value.toString()) === true) {
        const producer2 = kafka.producer({
          createPartitioner: Partitioners.DefaultPartitioner,
        });
        await producer2.connect();
        await producer2.send({
          topic: "checkedresult",
          messages: [
            {
              key: message.key.toString(),
              value: message.value.toString(),
            },
          ],
        });
      } else {
        //If the id is incorrect print the id and the messages info to the console.
        console.log(
          checkId(message.value.toString()) +
            ", ID was not sent to the channel checkedresult"
        );
        console.log({
          key: message.key.toString(),
          partition: message.partition,
          offset: message.offset,
          value: message.value.toString(),
        });
      }
    },
  });
};
run().catch(console.error);

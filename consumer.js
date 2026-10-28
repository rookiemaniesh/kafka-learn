import {kafka} from './client.js';
async function init() {
    const group = process.argv[2] || "user-1";
    const consumer=kafka.consumer({groupId:group})
    await consumer.connect();
    await consumer.subscribe({topic:"rider-updt",fromBeginning:true});
    await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      console.log(
        `${group}: [${topic}]: PART:${partition}:`,
        message.value.toString()
      );
    },
  });
}
init();

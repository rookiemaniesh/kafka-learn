import {kafka} from "./client.js";
async function init(){
    const admin=kafka.admin();
    console.log("connecting...");
    await admin.connect();
    console.log('connected!!');
    console.log('Topics Creating...');
    await admin.createTopics({
        topics:[{
            topic:"rider-updt",
            numPartitions:2,
        },],
    });
    console.log('Topics Created!!');
    console.log('Disconnecting...');
    await admin.disconnect();
}
init();
import {Kafka} from "kafkajs";
export const kafka=new Kafka({
    clientId:'kafka-learn',
    brokers:["localhost:9092"]
})
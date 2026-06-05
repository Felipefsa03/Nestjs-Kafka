import { Kafka } from "kafkajs";
import { Injectable } from "@nestjs/common";
import { OnModuleInit } from "@nestjs/common";
import { OnApplicationShutdown } from "@nestjs/common";
import { Consumer, ConsumerRunConfig, ConsumerSubscribeTopics } from "kafkajs";
import { kafkaBrokers } from "./kafka.config";


@Injectable()
export class ConsumerService implements OnApplicationShutdown{
  private readonly kafka = new Kafka({
    brokers: kafkaBrokers,
  });

  private readonly consumers: Consumer[] = [];

  async consume(topic: ConsumerSubscribeTopics, config: ConsumerRunConfig) {
    const consumer = this.kafka.consumer({ groupId: 'nestjs-kafka' });
    await consumer.connect();
    await consumer.subscribe(topic);
    await consumer.run(config);
    this.consumers.push(consumer);
  }
  async onApplicationShutdown() {
    for (const consumer of this.consumers) {
      await consumer.disconnect();
    }
  }
}
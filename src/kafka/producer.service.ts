import { Injectable, OnApplicationShutdown } from "@nestjs/common";
import { Kafka } from "kafkajs";
import { OnModuleInit } from "@nestjs/common";
import { ProducerRecord } from "kafkajs";
import { Producer } from "kafkajs";
import { kafkaBrokers } from "./kafka.config";


@Injectable()
export class ProducerService implements OnModuleInit, OnApplicationShutdown {
  private readonly kafka = new Kafka({
    brokers: kafkaBrokers,
  });

  private readonly producer: Producer = this.kafka.producer();

  async onModuleInit() {
    await this.producer.connect();
  }

  async produce(record: ProducerRecord){
    await this.producer.send(record);
  }

  async onApplicationShutdown() {
    // console.log('Shutting down producer...', signal);
    await this.producer.disconnect();
    console.log('Producer disconnected');
  }
}
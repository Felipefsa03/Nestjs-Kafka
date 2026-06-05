import { Injectable } from '@nestjs/common';
import { ProducerService } from './kafka/producer.service';
import { ConsumerService } from './kafka/consumer.service';
import { ConsumerSubscribeTopics } from 'kafkajs';

@Injectable()
export class AppService {

  constructor(private readonly producerService: ProducerService) {}
  async getHello() {
    await this.producerService.produce({
      topic: 'test',
      messages: [{ value: 'Hello World!' }],
    });
    
    return {
      message: 'Hello World!',
    };
  }
}

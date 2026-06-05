export const kafkaBrokers = (
  process.env.KAFKA_BROKERS ?? 'localhost:9092'
).split(',');

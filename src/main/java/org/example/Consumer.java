package org.example;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {
  public static void main(String[] args) {
    final String topic = args[0];

    var kafkaConsumer =
        new KafkaConsumer<String, String>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.GROUP_ID_CONFIG,
                Long.toString(ThreadLocalRandom.current().nextLong())));

    kafkaConsumer.subscribe(Collections.singletonList(topic));
    boolean updated = false;
    List<Integer> received = new ArrayList<>();
    while (!Thread.interrupted()) {
      for (var record : kafkaConsumer.poll(Duration.ofSeconds(1))) {
        System.out.printf("[%s] => [%s]%n", record.key(), record.value());

        int key = Integer.parseInt(record.key().split("-")[1]);
        int value = Integer.parseInt(record.value().split("-")[1]);

        if (key != value) throw new RuntimeException("WAT");
        received.add(value);

        updated = true;
      }

      if (updated) {
        System.out.printf(
            "[Total received %d, is ordered = %s]%n",
            received.size(),
            received.stream().sorted().collect(Collectors.toList()).equals(received));
        updated = false;
      }
    }
  }
}

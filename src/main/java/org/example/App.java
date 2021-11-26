package org.example;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class App {

  public static void main(String[] args) {
    try {
      execute(Argument.from(args));
    } catch (ParameterException e) {
      e.printStackTrace();
      Argument.help();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static void execute(Argument argument) {
    System.out.println(argument);
    if (argument.showHelp) {
      Argument.help();
    } else if (argument.testing) {
      runProducerBenchmark(argument);
    } else {
      runProducerStream(argument);
    }
  }

  static void runProducerBenchmark(final Argument argument) {
    double ms = benchmark(() -> runProducerStream(argument), 32);
    System.out.printf("[LOG] time = %.4fms%n", ms);
  }

  static void runProducerStream(final Argument argument) {
    ConcurrentRecordProvider concurrentRecordProvider =
        new ConcurrentRecordProvider(argument.topicName, argument.recordSize);

    try (var producer = new KafkaProducer<String, String>(propertiesOf(argument))) {
      IntStream.range(0, argument.recordCount)
          .parallel()
          .forEach(i -> producer.send(concurrentRecordProvider.next()));
    }
  }

  static void runProducerSingleThread(final Argument argument) {
    try (var producer = new KafkaProducer<String, String>(propertiesOf(argument))) {
      for (int i = 0; i < argument.recordCount; i++) {
        producer.send(new ProducerRecord<>(argument.topicName, "key-" + i, "value-" + i));
      }
    }
  }

  static double benchmark(Runnable runnable, int tries) {
    System.out.println("[LOG] warm up ...");
    LongStream.range(0, 8).forEach((i) -> runnable.run());

    System.out.println("[LOG] run benchmark ...");
    return LongStream.range(0, tries)
        .map(
            i -> {
              long start = System.nanoTime();
              runnable.run();
              long end = System.nanoTime();
              System.out.printf(
                  "[LOG] iteration %d/%d = %dms%n", i, tries - 1, (end - start) / 1000000);
              return end - start;
            })
        .map(i -> i / 1000000)
        .average()
        .orElseGet(() -> Double.NaN);
  }

  static Map<String, Object> propertiesOf(final Argument argument) {
    return Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        argument.bootstrapServer,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class,
        ProducerConfig.BUFFER_MEMORY_CONFIG,
        Runtime.getRuntime().freeMemory() / 2,
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        256,
        ProducerConfig.ACKS_CONFIG,
        "0",
        ProducerConfig.LINGER_MS_CONFIG,
        100);
  }

  static class Argument {
    @Parameter(names = {"--bootstrap.servers"})
    public String bootstrapServer = "localhost:9092";

    @Parameter(names = {"--topic"})
    public String topicName =
        "Benchmark-" + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);

    @Parameter(names = {"--records"})
    public int recordCount = 100000;

    @Parameter(names = {"--recordSize"})
    public int recordSize = 1024;

    @Parameter(names = "--help")
    public boolean showHelp = false;

    @Parameter(names = "--testing")
    public boolean testing = false;

    public static Argument from(String[] args) {
      var argument = new Argument();
      JCommander.newBuilder().addObject(argument).build().parse(args);
      return argument;
    }

    public static void help() {
      var argument = new Argument();
      JCommander.newBuilder().addObject(argument).build().usage();
    }

    @Override
    public String toString() {
      return "Argument{"
          + " \n bootstrapServer='"
          + bootstrapServer
          + '\''
          + ",\n topicName='"
          + topicName
          + '\''
          + ",\n recordCount="
          + recordCount
          + ",\n recordSize="
          + recordSize
          + ",\n showHelp="
          + showHelp
          + ",\n testing="
          + testing
          + '}';
    }
  }
}

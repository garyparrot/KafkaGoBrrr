package org.example;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class App {

  public static void main(String[] args) {
    try {
      Argument argument = Argument.from(args);
      System.out.println(argument);
      if (argument.showHelp) {
        Argument.help();
        return;
      }

      execute(argument);
    } catch (ParameterException e) {
      e.printStackTrace();
      Argument.help();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static void execute(Argument argument) {
    ConcurrentRecordProvider concurrentRecordProvider =
        new ConcurrentRecordProvider(argument.topicName, argument.recordSize);

    try (var producer = new KafkaProducer<String, String>(propertiesOf(argument))) {
      IntStream.range(0, argument.recordCount)
          .parallel()
          .forEach(i -> producer.send(concurrentRecordProvider.next()));
    }
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
        Runtime.getRuntime().freeMemory() / 8,
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        16,
        ProducerConfig.ACKS_CONFIG,
        "0",
        ProducerConfig.BATCH_SIZE_CONFIG,
        4096 * 50,
        ProducerConfig.LINGER_MS_CONFIG,
        100);
  }

  static class Argument {
    @Parameter(names = {"--brokers"})
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
          + "bootstrapServer='"
          + bootstrapServer
          + '\''
          + ", topicName='"
          + topicName
          + '\''
          + ", recordCount="
          + recordCount
          + ", recordSize="
          + recordSize
          + ", showHelp="
          + showHelp
          + '}';
    }
  }
}

package org.example;

import java.util.concurrent.ThreadLocalRandom;

public class Play {
  public static void main(String[] args) {
    String topic = "Branch-" + ThreadLocalRandom.current().nextLong();
    App.Argument argument = new App.Argument();
    argument.bootstrapServer = "localhost:9092";
    argument.topicName = topic;
    argument.recordSize = 1000;
    argument.recordCount = 100000;
    argument.showHelp = false;
    long l = System.nanoTime();
    App.execute(argument);
    long r = System.nanoTime();
    System.out.printf("Time spend %d%n", (r - l) / 1000000);

    Consumer.main(new String[] {topic});
  }
}

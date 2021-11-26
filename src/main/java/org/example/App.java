package org.example;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.concurrent.ThreadLocalRandom;

public class App {

  public static void execute(final Argument argument) {
    System.out.println(argument);
  }

  public static void main(String[] args) {
    execute(Argument.from(args));
  }

  static class Argument {
    @Parameter(names = {"--topic"})
    public String topicName = "Benchmark-" + ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);

    @Parameter(names = {"--records"})
    public int recordCount = 1000;

    @Parameter(names = {"--recordSize"})
    public int recordSize = 1024;

    public static Argument from(String[] args) {
      var argument = new Argument();
      JCommander.newBuilder()
              .addObject(argument)
              .build()
              .parse(args);
      return argument;
    }

    @Override
    public String toString() {
      return "Argument{" +
              "topicName='" + topicName + '\'' +
              ", recordCount=" + recordCount +
              ", recordSize=" + recordSize +
              '}';
    }
  }
}

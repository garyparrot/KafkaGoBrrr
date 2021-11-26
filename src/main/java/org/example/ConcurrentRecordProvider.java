package org.example;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

public class ConcurrentRecordProvider {

  private final String topic;
  private final AtomicLong sequence;
  private final List<Header> headers;

  public ConcurrentRecordProvider(String topic, int recordSize) {
    this.topic = topic;
    this.sequence = new AtomicLong();
    this.headers = List.of(new FixedSizePayload(recordSize));
  }

  public ProducerRecord<String, String> next() {
    long index = sequence.getAndIncrement();
    return new ProducerRecord<>(topic, null, "key-" + index, "value-" + index, headers);
  }

  public static class FixedSizePayload implements Header {

    private final byte[] dummy;

    public FixedSizePayload(int size) {
      this.dummy = new byte[size];
    }

    @Override
    public String key() {
      return "dummy";
    }

    @Override
    public byte[] value() {
      return dummy;
    }
  }
}

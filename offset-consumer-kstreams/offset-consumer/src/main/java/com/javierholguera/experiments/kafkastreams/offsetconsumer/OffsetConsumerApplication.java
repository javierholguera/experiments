package com.javierholguera.experiments.kafkastreams.offsetconsumer;

import com.google.gson.Gson;

import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.BaseKey;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.OffsetKey;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Produced;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;

public class OffsetConsumerApplication {

  private static final String inputTopic = "__consumer_offsets";
  private static final String outputTopic = "transformed-offset-consumers";
  private static final Gson gson = new Gson();

  /**
   * Application entry point.
   * @param args command line arguments collection.
   */
  public static void main(final String[] args) {

    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

    final Properties streamsConfiguration = new Properties();

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "offset-consumer-1");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "offset-consumer-client-1");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    //streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    //streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
    streamsConfiguration.put("exclude.internal.topics", "false"); // necessary to consume __consumer_offsets


    final StreamsBuilder builder = new StreamsBuilder();

    builder
        .stream(inputTopic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
        .map((k, v) -> KeyValue.pair(GroupMetadataManager.readMessageKey(ByteBuffer.wrap(k)), v))
        .filter(OffsetConsumerApplication::isTransformableKey)
        .map((k, v) -> KeyValue.pair((OffsetKey)k, v))
        .map(OffsetConsumerApplication::toJson)
        .to(Serdes.String(), Serdes.String(), outputTopic);

    KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
    kafkaStreams.start();

//    Properties consumerProperties = new Properties();
//    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "MyGroup-1");
//    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//    consumerProperties.put("exclude.internal.topics", "false");
//
//    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
//    consumer.subscribe(Collections.singleton(inputTopic));
//    while (true) {
//      ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
//      records.forEach(record -> {
//        System.out.println("record!");
//      });
//    }

  }

  private static boolean isTransformableKey(BaseKey key, byte[] v) {
    if (!(key instanceof OffsetKey)) {
      return false;
    }

    return ((OffsetKey)key).key().topicPartition().topic() != outputTopic;
  }

  private static KeyValue toJson(OffsetKey key, byte[] value) {
    if (value == null) {
      return null;
    }

    OffsetAndMetadata offsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value));

    ConsumerOffsetDetails details = new ConsumerOffsetDetails(
        key.key().topicPartition().topic(),
        key.key().topicPartition().partition(),
        key.key().group(),
        key.version(),
        offsetAndMetadata.offset(),
        offsetAndMetadata.metadata(),
        offsetAndMetadata.commitTimestamp(),
        offsetAndMetadata.expireTimestamp());

    return KeyValue.pair((String)null, gson.toJson(details));
  }
}

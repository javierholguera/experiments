package io.ninety.kafka.tools.recordremover;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RecordRemoverApplication {

    public static void main(final String[] args) throws Exception {

        CommandLine commandLine = parseCommandLine(args);
        if (commandLine == null)
            return;

        String brokerList = commandLine.getOptionValue("brokerlist");
        String topic = commandLine.getOptionValue("topic");
        String strategy = commandLine.getOptionValue("strategy");

        Properties config = new Properties();
        config.setProperty("bootstrap.servers", brokerList);
        AdminClient adminClient = KafkaAdminClient.create(config);

        if ("retention-based".equals(strategy)) {
            Long retentionMs = new Long(commandLine.getOptionValue("retention"));
            deleteByRetention(adminClient, topic, retentionMs);

        } else {
            throw new RuntimeException("Not implemented yet");
        }


        StringBuilder info = new StringBuilder();
        info.append("\n--------------------------------------------------------------------------------------------------\n");
        System.out.println(info.toString());
    }

    private static Config getTopicConfig(AdminClient adminClient, String topic) throws Exception {

        Map<ConfigResource, Config> configs = adminClient
                .describeConfigs(Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, topic)))
                .all()
                .get();

        if (configs.isEmpty()) {
            throw new RuntimeException("No topic found for param " + topic);
        }
        if (configs.size() > 1) {
            throw new RuntimeException("More than one topic found for param " + topic);
        }

        return configs.values().stream().findFirst().get();
    }

    private static void deleteByRetention(AdminClient adminClient, String topic, Long retentionMs) throws Exception {
        // retention.ms to let kafka truncate the topic and delete older records
        // cleanup.policy contains 'delete'

        Config topicConfig = getTopicConfig(adminClient, topic);
        ConfigEntry cleanupPolicyConfig = findEntry(topicConfig, "cleanup.policy");
        ConfigEntry retentionConfig = findEntry(topicConfig, "retention.ms");

        List<ConfigEntry> entriesToUpdate = new ArrayList<>();

        if (!cleanupPolicyConfig.value().contains("delete")) {

            // By default, cleanup.policy contains 'delete'. if it is not here, chances are it has been manually configured
            if (!cleanupPolicyConfig.isDefault()) {
                // Confirms that it has been manually set, we shouldn't change it automatically then.
                throw new RuntimeException(String.format(
                        "Topic %s has been manually configured to not allow 'delete' cleanup policy", topic));
            }

            // Seems like this is a value coming from the broker configuration, in which case it's find to change it
            if (cleanupPolicyConfig.value().contains("compact"))
                entriesToUpdate.add(new ConfigEntry("cleanup.policy", "compact,delete"));
            else
                entriesToUpdate.add(new ConfigEntry("cleanup.policy", "delete"));
        }

        entriesToUpdate.add(new ConfigEntry("retention.ms", retentionMs.toString()));

        // Wait for this to be sent to the brokers
        adminClient.alterConfigs(
                Collections.singletonMap(
                        new ConfigResource(ConfigResource.Type.TOPIC, topic),
                        new Config(entriesToUpdate)))
                .all()
                .get();

        // In theory we should not be able to read any message older than the retention specified. Let's see
        String brokerList = adminClient.describeCluster()
                .nodes().get().stream()
                .map(n -> String.format("%s:%s", n.host(), n.port()))
                .collect(Collectors.joining(","));
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "remove-record-checker-" + Instant.now().getEpochSecond());
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(consumerProperties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(Collections.singleton(topic));

        while (true) {

            Long ageLimit = Instant.now().toEpochMilli() - retentionMs;
            ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
            Iterable<ConsumerRecord<byte[], byte[]>> iterable = () -> records.iterator();
            Optional<ConsumerRecord<byte[], byte[]>> oldRecord = StreamSupport.stream(iterable.spliterator(), false)
                    .filter(record -> record.timestamp() < ageLimit)
                    .findFirst();
            if (oldRecord.isPresent()) {
                throw new RuntimeException(String.format(
                        "Found records that are older (%s) than the desired retention (%s)",
                        oldRecord.get().timestamp(),
                        retentionMs));
            }

            break;
        }

        // No old records anymore, restoring the previous configuration
        List<ConfigEntry> oldConfig = new ArrayList<>();
        oldConfig.add(cleanupPolicyConfig);
        oldConfig.add(retentionConfig);
        adminClient.alterConfigs(
                Collections.singletonMap(
                        new ConfigResource(ConfigResource.Type.TOPIC, topic),
                        new Config(oldConfig)))
                .all()
                .get();

        System.out.println(String.format(
                "Topic %s: records older than %s have been removed and configuration restored", topic, retentionMs));
    }

    private static void deleteByTombstone() {
        // delete.retention.ms = 0 to force tombstone markers to be removed.
        // Otherwise the consumers might still consume a record with the key and a null payload instead of nothing
    }

    private static ConfigEntry findEntry(Config topicConfig, String configName) {
        return topicConfig.entries().stream().filter(e -> configName.equals(e.name())).findFirst().get();
    }

    private static CommandLine parseCommandLine(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = createOptions();

        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            return line;
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("record-remover", createOptions());

            return null;
        }
    }

    private static Options createOptions() {
        Options options = new Options();

        options.addOption(Option.builder("brokerlist")
                .argName("BROKER-LIST")
                .desc("Comma-separated list of Kafka brokers")
                .hasArg(true)
                .required()
                .valueSeparator(',')
                .build());

        options.addOption(Option.builder("topic")
                .argName("TOPIC-NAME")
                .desc("Topic that contains the record to delete")
                .hasArg(true)
                .required()
                .build());

        options.addOption(Option.builder("strategy")
                .argName("STRATEGY-NAME")
                .desc("Deletion strategy (e.g. retention-based, message compaction)")
                .hasArg()
                .required()
                .build());

        options.addOption(Option.builder("retention")
                .argName("NEW-RETENTION-MS")
                .desc("New retention for records. Anything older than this value will be truncated")
                .hasArg()
                .required(false)
                .build());

        options.addOption(Option.builder("recordkey")
                .argName("RECORD-UNIQUE-KEY")
                .desc("Key of the record that will be deleted (after compacting the topic)")
                .hasArg()
                .required(false)
                .build());

        return options;
    }
}

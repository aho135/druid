import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FlinkKafkaConnector
{
  public static void main(String[] args) throws Exception
  {
    String sourceTopic = "events_source";
    String sinkTopic = "events_sink";
    String bootstrapServer = "kafka:9092";

    createTopic(bootstrapServer, sourceTopic);
    createTopic(bootstrapServer, sinkTopic);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    KafkaSource<String> source = KafkaSource.<String>builder()
                                            .setBootstrapServers(bootstrapServer)
                                            .setTopics(sourceTopic)
                                            .setGroupId("my-group-id")
                                            .setStartingOffsets(OffsetsInitializer.earliest())
                                            .setValueOnlyDeserializer(new SimpleStringSchema())
                                            .build();
    KafkaRecordSerializationSchema <String> serializer = KafkaRecordSerializationSchema.builder()
                                                                                       .setValueSerializationSchema(new SimpleStringSchema())
                                                                                       .setTopic(sinkTopic)
                                                                                       .build();
    KafkaSink<String> sink = KafkaSink.<String>builder()
                                      .setBootstrapServers(bootstrapServer)
                                      .setRecordSerializer(serializer)
                                      .build();
    DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceTopic);
    text.sinkTo(sink);
    env.execute("FlinkKafkaConnector");
  }

  static void createTopic(String bootstrapServer, String topicName)
  {
    int numPartitions = 3;
    short replicationFactor = 1;
    Properties properties = new Properties();
    properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    try (AdminClient adminClient = AdminClient.create(properties)) {
      NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
      CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
      result.all().get();
    } catch (InterruptedException | ExecutionException e) {
      System.out.println("Failed to create topic=" + topicName);
    }
  }
}
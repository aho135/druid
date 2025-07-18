import com.google.gson.JsonObject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FlinkKafkaProducer
{
  public static void main(String[] args) throws Exception
  {
    String sourceTopic = "events_source";
    String bootstrapServer = "kafka:9092";

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<String> stream = env.addSource(new SourceFunction<String>()
    {
      private volatile boolean isRunning = true;
      @Override
      public void run(SourceContext<String> sourceContext) throws Exception
      {
        while (isRunning) {
          JsonObject event = new JsonObject();
          event.addProperty("timestamp", System.currentTimeMillis());
          event.addProperty("traceId", RandomStringUtils.randomAlphanumeric(32));
          event.addProperty("spanId", RandomStringUtils.randomAlphanumeric(16));
          event.addProperty("dbCallCount", RandomUtils.nextInt(0, 10));
          event.addProperty("dbCallDuration", RandomUtils.nextInt(1000, 30000));
          sourceContext.collect(event.toString());
          Thread.sleep(60000);
        }
      }

      @Override
      public void cancel()
      {
        isRunning = false;
      }
    });
    KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
                                                                                      .setValueSerializationSchema(new SimpleStringSchema())
                                                                                      .setTopic(sourceTopic)
                                                                                      .build();
    KafkaSink<String> sink = KafkaSink.<String>builder()
                                      .setBootstrapServers(bootstrapServer)
                                      .setRecordSerializer(serializer)
                                      .build();
    stream.sinkTo(sink);
    env.execute("FlinkKafkaProducer");
  }
}
package org.mattiazaffalon;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import java.io.OutputStream;

import java.io.IOException;
import java.io.InputStream;

public class BeamKafkaIOExampleAvroDeserializers {

    public static class ObjectCoder extends CustomCoder<Object> {
        public void encode(Object value, OutputStream outStream) throws CoderException, java.io.IOException {
            outStream.write(value.toString().getBytes());
        }

        public Object decode(InputStream inStream)  throws CoderException, java.io.IOException {
           String str = new String(inStream.readAllBytes());

           return str;
        }
    }

    public static interface KafkaConnectOptions extends PipelineOptions {
        @Description("Kafka broker")
        @Default.String("localhost:9092")
        String getBroker();
        void setBroker(String broker);

        @Description("Kafka topic")
        @Default.String("beam-avro-topic")
        String getTopic();
        void setTopic(String topic);

        @Description("Window size mins")
        @Default.Long(1L)
        Long getWindowSizeMins();
        void setWindowSizeMins(Long mins);

        @Description("Output")
        @Default.String("/tmp/beam-kafka-avro-topic")
        String getOutput();
        void setOutput(String output);
    }

    static void runWindowedKafkaFetch(KafkaConnectOptions options) throws IOException {

        Pipeline pipeline = Pipeline.create(options);

        PCollection<Object> kafkaStringMessages = pipeline
                .apply(KafkaIO.<String, Object>read()
                        .withBootstrapServers(options.getBroker())
                        .withTopic(options.getTopic())  // use withTopics(List<String>) to read from multiple topics.
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializerAndCoder(KafkaAvroDeserializer.class, new ObjectCoder())

                        // Above four are required configuration. returns PCollection<KafkaRecord<Long, String>>

                        // Rest of the settings are optional :

                        // you can further customize KafkaConsumer used to read the records by adding more
                        // settings for ConsumerConfig. e.g :
                        .withConsumerConfigUpdates(ImmutableMap.of("group.id", "my_beam_app_1", "schema.registry.url", "http://localhost:8081"))

                        // set event times and watermark based on LogAppendTime. To provide a custom
                        // policy see withTimestampPolicyFactory(). withProcessingTime() is the default.
                        .withCreateTime(Duration.standardMinutes(1))

                        // restrict reader to committed messages on Kafka (see method documentation).
                        .withReadCommitted()

                        // offset consumed by the pipeline can be committed back.
                        .commitOffsetsInFinalize()

                        // finally, if you don't need Kafka metadata, you can drop it.g
                        .withoutMetadata() // PCollection<KV<Long, String>>
                )
                .apply(Values.create()); // PCollection<String>

        PCollection<Object> windowedKafkaStringMessages =
                kafkaStringMessages.apply(
                        Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSizeMins()))));

//        windowedKafkaStringMessages
//                .apply(new WriteOneFilePerWindow(options.getOutput(), 1));

        windowedKafkaStringMessages
                .apply(MapElements.into(TypeDescriptor.of(String.class)).via(
                        new ProcessFunction<Object, String>() {
                            public String apply(Object input) throws Exception {
                                return input.toString();
                            }
                        }
                ))
                .apply(TextIO.write()
                        .withWindowedWrites()
                        .withNumShards(1)
                        .to(options.getOutput()));

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }

    public static void main(String[] args) throws IOException {
        KafkaConnectOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaConnectOptions.class);

        runWindowedKafkaFetch(options);
    }
}

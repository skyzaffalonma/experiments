package org.mattiazaffalon;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.mattiazaffalon.common.WriteOneFilePerWindow;

import java.io.IOException;

public class BeamKafkaIOExampleBasicsDeserializers {
//    static final int WINDOW_SIZE = 10; // Default window duration in minutes
//    /**
//     * Concept #2: A DoFn that sets the data element timestamp. This is a silly method, just for this
//     * example, for the bounded data case.
//     *
//     * <p>Imagine that many ghosts of Shakespeare are all typing madly at the same time to recreate
//     * his masterworks. Each line of the corpus will get a random associated timestamp somewhere in a
//     * 2-hour period.
//     */
//    static class AddTimestampFn extends DoFn<String, String> {
//        private final Instant minTimestamp;
//        private final Instant maxTimestamp;
//
//        AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
//            this.minTimestamp = minTimestamp;
//            this.maxTimestamp = maxTimestamp;
//        }
//
//        @ProcessElement
//        public void processElement(@Element String element, OutputReceiver<String> receiver) {
//            Instant randomTimestamp =
//                    new Instant(
//                            ThreadLocalRandom.current()
//                                    .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis()));
//
//            /*
//             * Concept #2: Set the data element with that timestamp.
//             */
//            receiver.outputWithTimestamp(element, randomTimestamp);
//        }
//    }

//    /** A {@link DefaultValueFactory} that returns the current system time. */
//    public static class DefaultToCurrentSystemTime implements DefaultValueFactory<Long> {
//        @Override
//        public Long create(PipelineOptions options) {
//            return System.currentTimeMillis();
//        }
//    }
//
//    /** A {@link DefaultValueFactory} that returns the minimum timestamp plus one hour. */
//    public static class DefaultToMinTimestampPlusOneHour implements DefaultValueFactory<Long> {
//        @Override
//        public Long create(PipelineOptions options) {
//            return options.as(Options.class).getMinTimestampMillis()
//                    + Duration.standardHours(1).getMillis();
//        }
//    }

    public static interface KafkaConnectOptions extends PipelineOptions {
        @Description("Kafka broker")
        @Default.String("localhost:9092")
        String getBroker();
        void setBroker(String broker);

        @Description("Kafka topic")
        @Default.String("beam-string-topic")
        String getTopic();
        void setTopic(String topic);

        @Description("Window size mins")
        @Default.Long(1L)
        Long getWindowSizeMins();
        void setWindowSizeMins(Long mins);

        @Description("Output")
        @Default.String("file:///C/tmp/beam-kafka-string-topic")
        String getOutput();
        void setOutput(String output);
    }

    static void runWindowedKafkaFetch(KafkaConnectOptions options) throws IOException {

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> kafkaStringMessages = pipeline
                .apply(KafkaIO.<String, String>read()
                        .withBootstrapServers(options.getBroker())
                        .withTopic(options.getTopic())  // use withTopics(List<String>) to read from multiple topics.
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)

                        // Above four are required configuration. returns PCollection<KafkaRecord<Long, String>>

                        // Rest of the settings are optional :

                        // you can further customize KafkaConsumer used to read the records by adding more
                        // settings for ConsumerConfig. e.g :
                        .withConsumerConfigUpdates(ImmutableMap.of("group.id", "my_beam_app_1"))

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

        PCollection<String> windowedKafkaStringMessages =
                kafkaStringMessages.apply(
                        Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSizeMins()))));

       windowedKafkaStringMessages
               .apply(new WriteOneFilePerWindow(options.getOutput(), 1));

        // windowedKafkaStringMessages
        //         .apply(TextIO.write()
        //                 .withWindowedWrites()
        //                 .withNumShards(1)
        //                 .to(options.getOutput()));

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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Locale;
import java.util.Properties;

public class DataFlowStream {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "stream-dataflow");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream= builder.stream("streams-dataflow-input");
        stream.foreach((key , value) -> System.out.println("Key and Value : " + key + " " + value));
        KStream<String , String> fStream = stream.filter((k,v)-> v.contains("filter"))
                .mapValues((v)-> v.toUpperCase(Locale.ROOT));
        fStream.to("streams-dataflow-output");
        Topology build = builder.build();
        System.out.println(build.describe());

        KafkaStreams streams = new KafkaStreams(build,properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

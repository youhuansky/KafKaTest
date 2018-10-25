package KafKaTest.stream;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

public class Application {

	
	
		public static void main(String[] args) {

			// 定义输入的topic
	        String from = "first";
	        // 定义输出的topic
	        String to = "second";

	        // 设置参数
	        Properties settings = new Properties();
	        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
	        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
	        
	        StreamsConfig config = new StreamsConfig(settings);
	        
	        TopologyBuilder topologyBuilder = new TopologyBuilder();
	        topologyBuilder.addSource("Source", from).addProcessor("Process",   new ProcessorSupplier<byte[], byte[]>(){

				@Override
				public Processor<byte[], byte[]> get() {
					return new MyProcessor();
				}}, "Source").addSink("SINK", to, "Process");
	        KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder, config);
	        kafkaStreams.start();
	}
}

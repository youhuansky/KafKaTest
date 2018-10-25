package KafKaTest;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class NewProducer {

	// public static void main(String[] args) {
	// try {
	// // 创建生产者对象
	// Properties props = new Properties();
	// props.put("bootstrap.servers", "hadoop102:9092");
	// //当所有的从节点都返回接受成功的时候再返回ack标志
	// props.put("acks", "all");
	// //失败重试次数
	// props.put("retries", 0);
	// //批量处理数据的大小
	// props.put("batch.size", 16384);
	// //延时时间
	// props.put("linger.ms", 1);
	// //缓存大小
	// props.put("buffer.memory", 33554432);
	// props.put("key.serializer",
	// "org.apache.kafka.common.serialization.StringSerializer");
	// props.put("value.serializer",
	// "org.apache.kafka.common.serialization.StringSerializer");
	// Producer<String, String> producer = new KafkaProducer<>(props);
	// for (int i = 0; i < 100; i++) {
	// producer.send(new ProducerRecord<String, String>("first",
	// Integer.toString(i), Integer.toString(i)));
	// }
	//
	// producer.close();
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	//
	// }

	public static void main(String[] args) {
		try {
			// 创建生产者对象
			Properties props = new Properties();
			props.put("bootstrap.servers", "hadoop102:9092");
			// 当所有的从节点都返回接受成功的时候再返回ack标志
			props.put("acks", "all");
			// 失败重试次数
			props.put("retries", 0);
			// 批量处理数据的大小
			props.put("batch.size", 16384);
			// 延时时间
			props.put("linger.ms", 1);
			// 缓存大小
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			Producer<String, String> producer = new KafkaProducer<>(props);
			for (int i = 0; i < 100; i++) {
				producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)),new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						System.out.println(metadata.partition()+"   "+metadata.offset());
					}
				});
			}

			producer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}

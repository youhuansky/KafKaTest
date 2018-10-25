package KafKaTest;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducerIntercepter implements ProducerInterceptor<String, String> {

	@Override
	public void configure(Map<String, ?> configs) {
		
	}

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		String key = record.key();
		if(Integer.parseInt(key)%3==0) {
			
			return null;
		}else {
			return record;
		}
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		
	}

	@Override
	public void close() {
		
	}


}

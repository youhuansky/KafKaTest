package KafKaTest.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class MyProcessor implements Processor<byte[], byte[]>{

	private ProcessorContext context;
	
	@Override
	public void close() {
		
	}

	@Override
	public void init(ProcessorContext context) {
		this.context=context;
	}

	@Override
	public void process(byte[] key, byte[] value) {
		String string = new String(value);
		System.out.println(string);
		if(string.contains("atguigu")) {
			string = string.replace("atguigu", "miaomiao");
		}
		context.forward(key, string.getBytes());
	}

	@Override
	public void punctuate(long arg0) {
		
	}

}

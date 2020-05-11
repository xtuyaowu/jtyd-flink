package quickstart.stream.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;


public class CustomRichParallelSourceFunction extends RichParallelSourceFunction<Long>{
	boolean isRunning = true;
	long count = 1;
	
	@Override
	public void cancel() {
		// TODO Auto-generated method stub
		isRunning = false;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		// TODO Auto-generated method stub
		super.open(parameters);
	}
	
	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		super.close();
	}
	
	@Override
	public void run(SourceContext<Long> ctx) throws Exception {
		// TODO Auto-generated method stub
		while (true) {
			ctx.collect(count);
			count += 1;
			Thread.sleep(1000);
		}
	}

}

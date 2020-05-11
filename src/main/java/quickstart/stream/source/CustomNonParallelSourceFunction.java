package quickstart.stream.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;


public class CustomNonParallelSourceFunction implements SourceFunction<Long>{
	boolean isRunning = true;
	long count = 1;
	@Override
	public void run(SourceContext<Long> ctx) throws Exception {
		// TODO Auto-generated method stub
		while (true) {
			ctx.collect(count);
			count += 1;
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub
		isRunning = false;
	}

}

package quickstart.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

public class CounterApp {
	private void psvm() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSource<String> data = env.fromElements("hadoop","flink","spark","storm","kafka");
		
		DataSet<String> info =data.map(new RichMapFunction<String, String>() {
			
			//1.定义计数器
			LongCounter counter = new LongCounter();
			
			//2.注册计数器
			@Override
			public void open(Configuration parameters) throws Exception {
				// TODO Auto-generated method stub
				super.open(parameters);
				getRuntimeContext().addAccumulator("ele-counts", counter);
			}
			@Override
			public String map(String value) throws Exception {
				// TODO Auto-generated method stub
				counter.add(1);
				return value;
			}
		});
		
		String filePath = "D:\\backup\\Desktop\\aaa";
		info.writeAsText(filePath);
		
		JobExecutionResult executionResult = env.execute("CounterApp");
		
		//3.获取计数器
		Long num = executionResult.getAccumulatorResult("ele-counts");
		System.out.println("num:"+num);
		
		
	}
}

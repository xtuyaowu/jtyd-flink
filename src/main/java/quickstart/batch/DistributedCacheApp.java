package quickstart.batch;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

public class DistributedCacheApp {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		String filePath = "D:\\backup\\Desktop\\aaa";
		//1.注册一个本地文件
		env.registerCachedFile(filePath, "pk-java-dc");
		
		DataSource<String> data = env.fromElements("hadoop","flink","spark","storm","kafka");
		
		data.map(new RichMapFunction<String, String>() {
			//2.缓存文件
			List<String> list = new ArrayList<String>();
			@Override
			public void open(Configuration parameters) throws Exception {
				// TODO Auto-generated method stub
				super.open(parameters);
				File file = getRuntimeContext().getDistributedCache().getFile("pk-java-dc");
				List<String> lines = FileUtils.readLines(file);
				for (String line : lines) {
					list.add(line);
					System.out.println("line = ["+line+"]");
				}
				
			}
			@Override
			public String map(String value) throws Exception {
				// TODO Auto-generated method stub
				//3.可以在map中使用list
				return value;
			}
		}).print();
		
		
		
	}
}

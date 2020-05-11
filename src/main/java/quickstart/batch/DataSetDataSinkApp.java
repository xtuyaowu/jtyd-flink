package quickstart.batch;

import java.util.ArrayList;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem.WriteMode;

public class DataSetDataSinkApp {
	private void psvm() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		ArrayList<Integer> info = new ArrayList<Integer>();
		for (int i = 0; i < 10; i++) {
			info.add(i);
		}
		
		String filePath = "D:\\backup\\Desktop\\aaa\\";
		DataSource<Integer> data = env.fromCollection(info);
		data.writeAsText(filePath,WriteMode.OVERWRITE).setParallelism(2);
		env.execute("DataSetDataSinkApp");

	}
}

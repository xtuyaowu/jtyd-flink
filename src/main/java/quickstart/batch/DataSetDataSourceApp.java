package quickstart.batch;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;



public class DataSetDataSourceApp {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		//csvFile(env);
		//textFile(env);
		//fromCollection(env);
		fromInputFormat(env);
	}

	private static void fromInputFormat(ExecutionEnvironment env) {
		//env.createInput(new InputFormat());


	}

	private static void csvFile(ExecutionEnvironment env) throws Exception {
    	String file = "file://D:/backup/Desktop/aaa/flink.csv";
    	//env.readCsvFile(file,ignoreFirstLine=true).print();
    	String string = env.readCsvFile(file).ignoreFirstLine().toString();
    	System.out.println(string);

	}
	public static void textFile(ExecutionEnvironment env) throws Exception{
		String file = "file://D:/backup/Desktop/aaa/flink.txt";
		env.readTextFile(file).print();
		System.out.println("~~~~~~~~~~~~~~华丽的分割线~~~~~~~~~~~~~~~~");
		//file = "file://D://backup/Desktop/aaa/";
		//env.readTextFile(file).print();
	}
	
	public static void fromCollection(ExecutionEnvironment env) throws Exception {
			List<Integer> list= new ArrayList<Integer>();
			for (int i = 0; i < 10; i++) {
				list.add(i);
			}
			String file = "file://D:/backup/Desktop/aaa/flink0.txt";
			System.out.println("start-print");
			DataSource<Integer> collection = env.fromCollection(list); //.print()
			//collection.writeAsText(file);
			System.out.println("start-print");
			
			
			collection.print();
			System.out.println("end-print");
			
	}
	
		

}

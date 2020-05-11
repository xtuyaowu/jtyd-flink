package quickstart.batch;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_QUORUM;

public class HbaseSourceSinkApp {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, String>> hbaseInput =  env.createInput(new TableInputFormat<Tuple2<String, String>>(){
            Tuple2<String, String> tuple2 = new Tuple2<>();
            @Override
            public void configure(Configuration parameters) {
                TableName tableName = TableName.valueOf("logTest");
                String cf1 = "cf1";
                Connection conn = null;
                org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.property.clientPort", "2181");
                configuration.set("hbase.zookeeper.quorum", "106.54.142.79");
                //集群配置↓
                //configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
                //configuration.set("hbase.master", "106.54.142.79:60000");
                Connection connection = null;
                try {
                    connection = ConnectionFactory.createConnection(configuration);
                    Table table= connection.getTable(tableName);
//                    Scan scan = new Scan();
//                    scan.withStartRow(Bytes.toBytes("1001"));
//                    scan.withStopRow(Bytes.toBytes("1001"));
//                    scan.addFamily(Bytes.toBytes(cf1));
////                    ResultScanner results = table.getScanner(scan);
////                    for (Result result : results) {
////                        System.out.println(result);
////                    }
                    Get get = new Get("1001".getBytes()); //根据主键查询
                    Result result = table.get(get);
                    Cell[] cells = result.rawCells();
                    for (Cell c:cells) {
                        System.out.println(new String(c.getRow()));
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            protected Scan getScanner() {
                //Scan scan = new Scan();
                return scan;
            }

            @Override
            protected String getTableName() {
                String tablename = "logTest";
                return tablename;
            }

            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {
                String rowKey = Bytes.toString(result.getRow());
                StringBuffer sb = new StringBuffer();
                List<Cell> cells = result.listCells();
                for (Cell cell: cells){
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    sb.append(value).append("_");
                }
                String value = sb.replace(sb.length() - 1, sb.length(), "").toString();
                tuple2.setField(rowKey, 0);
                tuple2.setField(value, 1);
                return tuple2;

            }
        });

        hbaseInput.print();

    }
}

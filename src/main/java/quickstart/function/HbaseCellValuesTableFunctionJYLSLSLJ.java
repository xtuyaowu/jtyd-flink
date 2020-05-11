package quickstart.function;



import org.apache.flink.table.functions.TableFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

public class HbaseCellValuesTableFunctionJYLSLSLJ extends TableFunction<String> {
    String zkHosts = "master.northking.com,slave1.northking.com,slave2.northking.com";
    String zkPort = "2181";

    public void eval(String hTableName, String familyName, String cellName, String rowKey) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, zkHosts);
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);

        Connection conn = ConnectionFactory.createConnection(config);
        TableName tableName = TableName.valueOf(hTableName);
        Table table = conn.getTable(tableName);

        getTableCellsByRowKey(familyName, cellName, rowKey, table);

    }


    public void getTableCellsByRowKey(String familyName, String cellName, String rowKey, Table table) throws IOException, ClassNotFoundException {
        Scan scan = new Scan();
        scan.setStartRow(rowKey.getBytes());
        scan.setStopRow("3".getBytes());
        ResultScanner scanner = table.getScanner(scan);
        Double rs = 0.0;
        System.out.println("获得结果："+scanner);
        for (Result result:scanner) {
            rs+=Double.parseDouble(Bytes.toString(result.getValue(familyName.getBytes(), cellName.getBytes())));
        }
        collect(rs.toString());
    }


}

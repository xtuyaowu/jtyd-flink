package quickstart.function;


import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import quickstart.pojo.AsyncUser;
import quickstart.pojo.JYLSLSLJ;
import quickstart.pojo.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HbaseAsyncTableFunction extends AsyncTableFunction<User> {
    Connection conn =null;
    String zkHosts = "master.northking.com,slave1.northking.com,slave2.northking.com";
    String zkPort = "2181";
    private AsyncUser asyncUser;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, zkHosts);
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        conn = ConnectionFactory.createConnection(config);
    }

    public void eval(String hTableName, String familyName, String cellNames, String type, String rowKey) throws Exception {

        TableName tableName = TableName.valueOf(hTableName);
        List<JYLSLSLJ> value = null;
        Table table = conn.getTable(tableName);

        getTableCellsByRowKey(familyName, cellNames, rowKey, table);

    }


    public void getTableCellsByRowKey(String familyName, String cellNames, String rowKey, Table table) throws IOException, ClassNotFoundException {
        Scan scan = new Scan();
        scan.setStartRow(rowKey.getBytes());
        scan.setStopRow("3".getBytes());
        ResultScanner scanner = table.getScanner(scan);
        User user = new User();
        System.out.println("获得结果："+scanner);
        for (Result result:scanner) {
            ArrayList<HashMap<String, String>> hashMaps = new ArrayList<>();
            user.setId(Bytes.toString(result.getRow()));
            user.setGender(Bytes.toString(result.getValue(familyName.getBytes(), "sex".getBytes())));
            user.setUsername(Bytes.toString(result.getValue(familyName.getBytes(), "customerName".getBytes())));
            user.setAge(Bytes.toString(result.getValue(familyName.getBytes(), "age".getBytes())));
            System.out.println("User信息："+user.toString());
            //collect(user);
        }
    }


}

package quickstart.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

/**
 * @author ：jichangyu
 * @date ：Created in 2020/4/23 16:27
 * @description：read hive examples
 * @modified By：
 * @version: 1.0
 */
public class HivetoHiveApp {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner() // 使用BlinkPlanner
                .inBatchMode() // Batch模式，默认为StreamingMode
                .build();
        //使用StreamingMode
       /* EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner() // 使用BlinkPlanner
                .inStreamingMode() // StreamingMode
                .build();*/

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name = "myhive";      // Catalog名称，定义一个唯一的名称表示
        String defaultDatabase = "ors";  // 默认数据库名称
        String hiveConfDir = "D:\\日常工作\\hive\\conf";
        /* hive-site.xml路径 其中必须配置
        <property>
          <name>hive.metastore.uris</name>  //注意这里是false, 不是本地模式
          <value>thrift://host2:9083</value>    //host2为开启hiveserver2的host
        </property>
        */
        String version = "2.3.6";       // Hive版本号

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
        // 创建数据库，目前不支持创建hive表
//        String createSql ="CREATE DATABASE IF NOT EXISTS myhive.test20200424";
//        tableEnv.sqlUpdate(createSql);
        String createSql ="CREATE TABLE ors.test20200424(hxjlsh string,khmc string)";
        tableEnv.sqlUpdate(createSql);

//        String querySql = "select hxjylsh from myhive.ors.t_i_a_jylsb where hxjylsh = '1001908'";
//        tableEnv.sqlQuery(querySql);
//        String updateSql ="insert into myhive.ors.t_i_a_jylsb_202004231 select * from myhive.ors.t_i_a_jylsb where hxjylsh = '1001908'";//需要指定catalog名+库名
//        tableEnv.sqlUpdate(updateSql);

        String updateSql ="insert into ors.t_i_a_jylsb_202004231(hxjylsh) values('1001908')";//需要指定catalog名+库名
        tableEnv.sqlUpdate(updateSql);
//        String updateSql ="insert into myhive.ors.t_i_a_jylsb_20200423 values ('12312794','aaa')";
//        tableEnv.sqlUpdate(updateSql);


//        Table table = bTEnv.sqlQuery(querySql);
//        DataSet<Row> rowDataSet = bTEnv.toDataSet(table, Row.class);
//        rowDataSet.print();
//        String createDbSql = "CREATE DATABASE IF NOT EXISTS myhive.test123";
//
//        tableEnv.sqlUpdate(createDbSql);
        //DataSet<Row> result = tableEnv.toDataSet(table,Row.class);
        //result.print();
//        table.printSchema();
//        env.execute("myhivetest");

    }
}

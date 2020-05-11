package quickstart.function;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;

/**
 * @author ：jichangyu
 * @date ：Created in 2020/4/21 16:02
 * @description：hbase异步查询维表
 * @modified By：
 * @version: 1.0
 */
public class HbaseAsyncTableSource implements LookupableTableSource {

    @Override
    public TableFunction getLookupFunction(String[] strings) {
        return null;
    }

    @Override
    public AsyncTableFunction getAsyncLookupFunction(String[] strings) {
        return null;
    }

    @Override
    public boolean isAsyncEnabled() {
        return false;
    }


    @Override
    public TableSchema getTableSchema() {
        return null;
    }
}

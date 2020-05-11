package quickstart.batch.source;

import org.apache.flink.addons.hbase.TableInputSplit;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;

public class HbaseSourceFunction<T> extends RichInputFormat<T, TableInputSplit> {
    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public TableInputSplit[] createInputSplits(int i) throws IOException {
        return new TableInputSplit[0];
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(TableInputSplit[] tableInputSplits) {
        return null;
    }

    @Override
    public void open(TableInputSplit tableInputSplit) throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public T nextRecord(T t) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}

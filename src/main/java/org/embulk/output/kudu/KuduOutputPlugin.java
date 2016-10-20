package org.embulk.output.kudu;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.kudu.setter.BinaryColumnSetter;
import org.embulk.output.kudu.setter.BoolColumnSetter;
import org.embulk.output.kudu.setter.ColumnSetter;
import org.embulk.output.kudu.setter.ColumnSetterVisitor;
import org.embulk.output.kudu.setter.DoubleColumnSetter;
import org.embulk.output.kudu.setter.FloatColumnSetter;
import org.embulk.output.kudu.setter.Int16ColumnSetter;
import org.embulk.output.kudu.setter.Int32ColumnSetter;
import org.embulk.output.kudu.setter.Int64ColumnSetter;
import org.embulk.output.kudu.setter.Int8ColumnSetter;
import org.embulk.output.kudu.setter.SkipColumnSetter;
import org.embulk.output.kudu.setter.StringColumnSetter;
import org.embulk.output.kudu.setter.UnixTimeMicroColumnSetter;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.util.Timestamps;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings("unused")
public class KuduOutputPlugin
        implements OutputPlugin {
    public interface PluginTask
            extends Task, TimestampFormatter.Task {
        @Config("masters")
        List<MasterAddressTask> getMasters();

        @Config("table")
        String getTable();

        @Config("column_options")
        @ConfigDefault("{}")
        Map<String, TimestampColumnOption> getColumnOptions();
    }

    public interface MasterAddressTask
            extends Task {
        @Config("host")
        String getHost();

        @Config("port")
        @ConfigDefault("7051")
        int getPort();

    }

    public interface TimestampColumnOption
            extends Task, TimestampFormatter.TimestampColumnOption {
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  Schema schema, int taskCount,
                                  OutputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int taskCount,
                             OutputPlugin.Control control) {
        throw new UnsupportedOperationException("kudu output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        Schema schema, int taskCount,
                        List<TaskReport> successTaskReports) {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex) {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        KuduClient client = createClient(task);
        KuduTable table = openTable(client, task.getTable());

        TimestampFormatter[] timestampFormatters = Timestamps.newTimestampColumnFormatters(task, schema, task.getColumnOptions());

        PageReader pageReader = new PageReader(schema);

        return new KuduPageOutput(pageReader, client, table, timestampFormatters);
    }

    public static class KuduPageOutput implements TransactionalPageOutput {
        private final PageReader pageReader;
        private final KuduClient client;
        private final KuduSession session;
        private final KuduTable table;
        private final TimestampFormatter[] timestampFormatters;
        private final List<ColumnSetter> setters;

        public KuduPageOutput(PageReader pageReader, KuduClient client, final KuduTable table, TimestampFormatter[] timestampFormatters) {
            this.pageReader = pageReader;
            this.client = client;
            this.session = client.newSession();
            this.table = table;
            this.timestampFormatters = timestampFormatters;

            final org.apache.kudu.Schema kuduSchema = table.getSchema();

            this.setters = Lists.transform(pageReader.getSchema().getColumns(), new Function<Column, ColumnSetter>() {
                @Nullable
                @Override
                public ColumnSetter apply(@Nullable Column input) {
                    checkNotNull(input);
                    return newColumnSetter(input, kuduSchema);
                }
            });

            this.session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        }

        @Override
        public void add(Page page) {
            try {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    Insert insert = table.newInsert();
                    PartialRow row = insert.getRow();
                    fillRow(row, pageReader);
                    session.apply(insert);
                }
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void finish() {
            try {
                session.close();
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void close() {
            try {
                client.close();
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void abort() {
        }

        @Override
        public TaskReport commit() {
            return Exec.newTaskReport();
        }

        private void fillRow(PartialRow row, PageReader record) {
            record.getSchema().visitColumns(new ColumnSetterVisitor(record, setters, row));
        }

        private ColumnSetter newColumnSetter(Column column, org.apache.kudu.Schema schema) {
            ColumnSetter setter;
            try {
                int index = schema.getColumnIndex(column.getName());
                ColumnSchema columnSchema = schema.getColumnByIndex(index);
                switch (columnSchema.getType()) {
                    case BOOL:
                        setter = new BoolColumnSetter(index);
                        break;
                    case INT8:
                        setter = new Int8ColumnSetter(index);
                        break;
                    case INT16:
                        setter = new Int16ColumnSetter(index);
                        break;
                    case INT32:
                        setter = new Int32ColumnSetter(index);
                        break;
                    case INT64:
                        setter = new Int64ColumnSetter(index);
                        break;
                    case UNIXTIME_MICROS:
                        setter = new UnixTimeMicroColumnSetter(index);
                        break;
                    case FLOAT:
                        setter = new FloatColumnSetter(index);
                        break;
                    case DOUBLE:
                        setter = new DoubleColumnSetter(index);
                        break;
                    case STRING:
                        setter = new StringColumnSetter(index, timestampFormatters[column.getIndex()]);
                        break;
                    case BINARY:
                        setter = new BinaryColumnSetter(index);
                        break;
                    default: // never come here. to suppress compiler warning.
                        setter = new SkipColumnSetter();
                }
            } catch (IllegalArgumentException e) {
                setter = new SkipColumnSetter();
            }
            return setter;
        }
    }

    private KuduClient createClient(PluginTask task) {
        List<String> masters = getMasterStrings(task.getMasters());
        return new KuduClient.KuduClientBuilder(masters).build();
    }

    private List<String> getMasterStrings(List<MasterAddressTask> masters) {
        return Lists.transform(masters, new Function<MasterAddressTask, String>() {
            @Override
            @Nullable
            public String apply(@Nullable MasterAddressTask input) {
                checkNotNull(input);
                return input.getHost() + ":" + input.getPort();
            }
        });
    }

    private KuduTable openTable(KuduClient client, String tableName) {
        KuduTable kuduTable = null;
        try {
            kuduTable = client.openTable(tableName);
        } catch (Exception e) {
            Throwables.propagate(e);
        }
        return kuduTable;
    }

    private void createTable(Schema schema, KuduClient client) {
    }

}

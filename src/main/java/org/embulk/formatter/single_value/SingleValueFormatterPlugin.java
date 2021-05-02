package org.embulk.formatter.single_value;

import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.embulk.spi.Exec;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.config.TaskSource;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.FileOutput;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.text.Newline;
import org.embulk.util.timestamp.TimestampFormatter;
import org.embulk.util.text.LineEncoder;

import org.msgpack.value.Value;

public class SingleValueFormatterPlugin
        implements FormatterPlugin
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
            .builder()
            .addDefaultModules()
            .build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    public interface PluginTask
            extends Task
    {
        @Config("column_name")
        @ConfigDefault("null")
        public Optional<String> getColumnName();

        @Config("null_string")
        @ConfigDefault("\"\"")
        String getNullString();

        @Config("timezone")
        @ConfigDefault("\"UTC\"")
        public String getTimezone();

        @Config("timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%6N %z\"")
        public String getTimestampFormat();

        // From org.embulk.spi.util.LineEncoder.Task
        @Config("charset")
        @ConfigDefault("\"utf-8\"")
        public Charset getCharset();

        // From org.embulk.spi.util.LineEncoder.Task
        @Config("newline")
        @ConfigDefault("\"CRLF\"")
        public Newline getNewline();

    }

    @Override
    public void transaction(ConfigSource config, Schema schema,
            FormatterPlugin.Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        // TODO: Use task.toTaskSource() after dropping v0.9
        control.run(task.dump());
    }

    private int getInputColumnIndex(Optional<String> columnName, Schema inputSchema)
    {
        if (columnName.isPresent()) {
            return inputSchema.lookupColumn(columnName.get()).getIndex();
        }
        return 0; // default is the first column
    }

    private Schema getOutputSchema(int inputColumnIndex, Schema inputSchema)
    {
        Column outputColumn = inputSchema.getColumn(inputColumnIndex);
        List<Column> columns = Collections.unmodifiableList(Arrays.asList(outputColumn));
        return new Schema(columns);
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
            final FileOutput output)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
        final LineEncoder encoder = LineEncoder.of(output, task.getNewline(), task.getCharset(), Exec.getBufferAllocator());
        final String nullString = task.getNullString();

        final int inputColumnIndex = getInputColumnIndex(task.getColumnName(), inputSchema);
        final Schema outputSchema = getOutputSchema(inputColumnIndex, inputSchema);
        final String timezone  = task.getTimezone();
        final TimestampFormatter timestampFormatter =
            createTimestampFormatter(task.getTimestampFormat(), timezone);

        // create a file
        encoder.nextFile();

        return new PageOutput() {
            // TODO: Use Exec.getPageReader() after dropping v0.9
            private final PageReader pageReader = new PageReader(inputSchema);

            public void add(Page page)
            {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    outputSchema.visitColumns(new ColumnVisitor() {
                        public void booleanColumn(Column column)
                        {
                            if (!pageReader.isNull(inputColumnIndex)) {
                                addValue(Boolean.toString(pageReader.getBoolean(inputColumnIndex)));
                            }
                            else {
                                addNullString();
                            }
                        }

                        public void longColumn(Column column)
                        {
                            if (!pageReader.isNull(inputColumnIndex)) {
                                addValue(Long.toString(pageReader.getLong(inputColumnIndex)));
                            }
                            else {
                                addNullString();
                            }
                        }

                        public void doubleColumn(Column column)
                        {
                            if (!pageReader.isNull(inputColumnIndex)) {
                                addValue(Double.toString(pageReader.getDouble(inputColumnIndex)));
                            }
                            else {
                                addNullString();
                            }
                        }

                        public void stringColumn(Column column)
                        {
                            if (!pageReader.isNull(inputColumnIndex)) {
                                addValue(pageReader.getString(inputColumnIndex));
                            }
                            else {
                                addNullString();
                            }
                        }

                        public void timestampColumn(Column column)
                        {
                            if (!pageReader.isNull(inputColumnIndex)) {
                                Instant value = pageReader.getTimestampInstant(inputColumnIndex);
                                addValue(timestampFormatter.format(value));
                            }
                            else {
                                addNullString();
                            }
                        }

                        public void jsonColumn(Column column)
                        {
                            if (!pageReader.isNull(inputColumnIndex)) {
                                Value value = pageReader.getJson(inputColumnIndex);
                                addValue(value.toJson());
                            }
                            else {
                                addNullString();
                            }
                        }

                        private void addValue(String v)
                        {
                            encoder.addText(v);
                        }

                        private void addNullString()
                        {
                            encoder.addText(nullString);
                        }
                    });
                    encoder.addNewLine();
                }
            }

            public void finish()
            {
                encoder.finish();
            }

            public void close()
            {
                encoder.close();
            }
        };
    }

    private TimestampFormatter createTimestampFormatter(String format, String timezone)
    {
        return TimestampFormatter.builder(format, true)
                .setDefaultZoneFromString(timezone)
                .setDefaultDateFromString("1970-01-01")
                .build();
    }
}

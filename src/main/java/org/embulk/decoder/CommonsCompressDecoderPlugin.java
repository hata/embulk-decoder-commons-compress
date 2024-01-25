package org.embulk.decoder;

import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.DecoderPlugin;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.util.file.FileInputInputStream;

public class CommonsCompressDecoderPlugin
        implements DecoderPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("format")
        @ConfigDefault("\"\"")
        public String getFormat();

        @Config("decompress_concatenated")
        @ConfigDefault("true")
        public boolean getDecompressConcatenated();

        @Config("match_name")
        @ConfigDefault("\"\"")
        public String getMatchName();
    }

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();

    @Override
    public void transaction(ConfigSource config, DecoderPlugin.Control control)
    {
        control.run(getTask(config).toTaskSource());
    }

    @Override
    public FileInput open(TaskSource taskSource, FileInput input)
    {
        final PluginTask task = getTask(taskSource);
        return new CommonsCompressFileInput(
                getBufferAllocator(),
                new CommonsCompressProvider(task, new FileInputInputStream(input) {
                    // NOTE: This is workaround code to avoid hanging issue.
                    // This issue will be fixed after merging #112.
                    // https://github.com/embulk/embulk/pull/112
                    @Override
                    public long skip(long len) {
                        long skipped = super.skip(len);
                        return skipped > 0 ? skipped : 0;
                    }
                }));
    }

    BufferAllocator getBufferAllocator() {
        return Exec.getBufferAllocator();
    }

    PluginTask getTask(ConfigSource config) {
        final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        return configMapper.map(config, PluginTask.class);
    }

    PluginTask getTask(TaskSource taskSource) {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        return taskMapper.map(taskSource, PluginTask.class);
    }
}

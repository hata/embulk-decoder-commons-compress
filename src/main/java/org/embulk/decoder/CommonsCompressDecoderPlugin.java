package org.embulk.decoder;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.DecoderPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.util.FileInputInputStream;

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

        @Config("pass_uncompress_file")
        @ConfigDefault("false")
        public boolean getPassUncompressFile();

        @Config("match_name")
        @ConfigDefault("\"\"")
        public String getMatchName();

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    @Override
    public void transaction(ConfigSource config, DecoderPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        control.run(task.dump());
    }

    @Override
    public FileInput open(TaskSource taskSource, FileInput input)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new CommonsCompressFileInput(
                task.getBufferAllocator(),
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
}

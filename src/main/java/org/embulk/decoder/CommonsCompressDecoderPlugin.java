package org.embulk.decoder;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.embulk.config.Config;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.DecoderPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.util.FileInputInputStream;
import org.embulk.spi.util.InputStreamFileInput.Provider;

public class CommonsCompressDecoderPlugin
        implements DecoderPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("format")
        // @ConfigDefault("") A default value will be enabled after implementing markSupport.
        public String getFormat();

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
        return new ArchiveInputStreamFileInput(
                task.getBufferAllocator(),
                new ArchiveInputStreamProvider(task, new FileInputInputStream(input)));
    }
    
    
    private static class ArchiveInputStreamFileInput implements FileInput {
        private final BufferAllocator allocator;
        private final Provider provider;
        private InputStream current;

        public ArchiveInputStreamFileInput(BufferAllocator allocator, Provider provider)
        {
            this.allocator = allocator;
            this.provider = provider;
            this.current = null;
        }

        @Override
        public Buffer poll()
        {
            // TODO check current != null and throw Illegal State - file is not opened
            if (current == null) {
                throw new IllegalStateException("nextFile() must be called before poll()");
            }
            Buffer buffer = allocator.allocate();
            try {
                int n = current.read(buffer.array(), buffer.offset(), buffer.capacity());
                if (n < 0) {
                    return null;
                }
                buffer.limit(n);
                Buffer b = buffer;
                buffer = null;
                return b;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                if (buffer != null) {
                    buffer.release();
                    buffer = null;
                }
            }
        }

        @Override
        public boolean nextFile()
        {
            try {
                if (current != null) {
                    // NOTE: DO NOT close current because this stream may
                    // be one of a file in a archive. Provider manage
                    // this stream.
                    current = null;
                }
                current = provider.openNext();
                return current != null;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void close()
        {
            try {
                provider.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
    
    private static class ArchiveInputStreamProvider implements Provider
    {
        private final PluginTask task;
        private final FileInputInputStream files;
        private final boolean autoFormatDetection;
        private ArchiveInputStream archiveInputStream;

        ArchiveInputStreamProvider(PluginTask task, FileInputInputStream files)
        {
            this.task = task;
            this.files = files;
            autoFormatDetection = task == null || task.getFormat() == null || task.getFormat().length() == 0;
        }

        @Override
        public InputStream openNext() throws IOException
        {
            try {
                while (true) {
                    if (archiveInputStream == null) {
                        if (!files.nextFile()) {
                            return null;
                        }
                        archiveInputStream = createArchiveInputStream();
                    } else {
                        ArchiveEntry entry = archiveInputStream.getNextEntry();
                        if (entry == null) {
                            archiveInputStream = null;
                        } else if (entry.isDirectory()) {
                            // Skip a directory.
                            continue;
                        } else {
                            return archiveInputStream;
                        }
                    }
                }            
            } catch (ArchiveException e) {
                throw new IOException(e);
            }
        }
        
        @Override
        public void close() throws IOException
        {
            if (archiveInputStream != null) {
                // Note: archiveInputStream is got from files. So, this stream
                // do not need to close here.
                archiveInputStream = null;
            }
            if (files != null) {
                files.close();
            }
        }

        private ArchiveInputStream createArchiveInputStream() throws IOException, ArchiveException
        {
            ArchiveStreamFactory factory = new ArchiveStreamFactory();
            if (autoFormatDetection) {
                if (!files.markSupported()) {
                    // TODO: Implement auto detection.
                    // TODO: files Need to support mark to use auto detection.
                    throw new IOException("Mark is not supported.");
                }
                return factory.createArchiveInputStream(files);
            } else {
                return factory.createArchiveInputStream(task.getFormat(), files);
            }
        }
    }
    
}

package org.embulk.decoder;

import java.io.IOException;
import java.io.InputStream;

import org.embulk.spi.Buffer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.FileInput;
import org.embulk.spi.util.InputStreamFileInput.Provider;


class CommonsCompressFileInput implements FileInput
{
    private final BufferAllocator allocator;
    private final Provider provider;
    private InputStream current;

    public CommonsCompressFileInput(BufferAllocator allocator, Provider provider)
    {
        this.allocator = allocator;
        this.provider = provider;
        this.current = null;
    }

    @Override
    public Buffer poll()
    {
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
            // NOTE: DO NOT close current because this stream may
            // be one of a file in an archive. Provider manage it.
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

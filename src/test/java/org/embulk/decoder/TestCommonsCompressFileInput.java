package org.embulk.decoder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;

import org.embulk.spi.Buffer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.util.InputStreamFileInput.Provider;
import org.junit.Test;

public class TestCommonsCompressFileInput {

    @Mocked BufferAllocator allocator;
    @Mocked Provider provider;

    @Test
    public void testPoll(@Mocked final InputStream in, @Mocked final Buffer allocBuffer) throws Exception {
        final byte[] bytes = new byte[]{'f', 'o', 'o'};
        final int readLength = 3;

        new NonStrictExpectations() {{
            provider.openNext(); result = in;
            allocator.allocate(); result = allocBuffer;
            allocBuffer.array(); result = bytes;
            allocBuffer.offset(); result = 0;
            allocBuffer.capacity(); result = bytes.length;
            allocBuffer.limit(readLength); result = allocBuffer;
            allocBuffer.release();
            in.read(bytes, 0, bytes.length); result = readLength;
        }};
        
        CommonsCompressFileInput input = new CommonsCompressFileInput(allocator, provider);
        assertTrue("Verify there is a new stream.", input.nextFile());
        assertTrue("Verify allocated buffer is returned.", allocBuffer == input.poll());
        input.close();

        new Verifications() {{
            allocBuffer.limit(readLength); times = 1;
            allocBuffer.release(); times = 0;
        }};
    }

    @Test(expected=RuntimeException.class)
    public void testPollThrowsException(@Mocked final InputStream in, @Mocked final Buffer allocBuffer) throws Exception {
        final byte[] bytes = new byte[]{'f', 'o', 'o'};

        new NonStrictExpectations() {{
            provider.openNext(); result = in;
            allocator.allocate(); result = allocBuffer;
            allocBuffer.array(); result = bytes;
            allocBuffer.offset(); result = 0;
            allocBuffer.capacity(); result = bytes.length;
            in.read(bytes, 0, bytes.length); result = new IOException("read throws IOException.");
        }};
        
        try {
            CommonsCompressFileInput input = new CommonsCompressFileInput(allocator, provider);
            assertTrue("Verify there is a new stream.", input.nextFile());
            input.poll();
            input.close();
        } finally {
            new Verifications() {{
                allocBuffer.release(); times = 1;
            }};
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testPollWithoutNextFile() {
        CommonsCompressFileInput input = new CommonsCompressFileInput(allocator, provider);
        input.poll();
        input.close();
    }

    @Test
    public void testNextFile(@Mocked final InputStream in) throws Exception {
        new NonStrictExpectations() {{
            provider.openNext(); result = in; result = null;
            provider.close();
        }};
        
        CommonsCompressFileInput input = new CommonsCompressFileInput(allocator, provider);
        assertTrue("Return a stream by a provider.", input.nextFile());
        assertFalse("Return no stream by a provider.", input.nextFile());
        input.close();

        new Verifications() {{
            in.close(); times = 0;
            provider.close(); times = 1;
        }};
    }

    @Test(expected=RuntimeException.class)
    public void testNextFileThrowsException() throws Exception {
        new NonStrictExpectations() {{
            provider.openNext(); result = new IOException("openNext throws IOException.");
            provider.close();
        }};
        
        CommonsCompressFileInput input = new CommonsCompressFileInput(allocator, provider);
        input.nextFile();
        input.close();
    }

    @Test
    public void testClose() throws Exception {
        new NonStrictExpectations() {{
            provider.close();
        }};
        
        CommonsCompressFileInput input = new CommonsCompressFileInput(allocator, provider);
        input.close();

        new Verifications() {{
            provider.close(); times = 1;
        }};
    }

    @Test
    public void testCloseDoNotCloseStream(@Mocked final InputStream in) throws Exception {
        new NonStrictExpectations() {{
            provider.openNext(); result = in;
            provider.close();
        }};
        
        CommonsCompressFileInput input = new CommonsCompressFileInput(allocator, provider);
        assertTrue("Return a stream", input.nextFile());
        input.close();

        new Verifications() {{
            in.close(); times = 0;
            provider.close(); times = 1;
        }};
    }

    @Test(expected=RuntimeException.class)
    public void testCloseThrowsRuntimeException() throws Exception {
        new NonStrictExpectations() {{
            provider.close(); result = new IOException("close throws IOException.");
        }};
        
        CommonsCompressFileInput input = new CommonsCompressFileInput(allocator, provider);
        input.close();
    }
}

package org.embulk.decoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.embulk.decoder.CommonsCompressDecoderPlugin.PluginTask;
import org.embulk.spi.util.FileInputInputStream;
import org.junit.Test;

public class TestCommonsCompressProvider {
    @Mocked PluginTask task;
    @Mocked FileInputInputStream files;

    @Test
    public void testCommonsCompressProviderAutoDetect() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "";
        }};

        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            assertTrue("Auto-detect is set.", provider.isFormatAutoDetection());
            assertNull("formats is set to null", provider.getFormats());
        }
    }

    @Test
    public void testCommonsCompressProviderSetFormat() throws Exception {
        new NonStrictExpectations() {{
            task.getFormat(); result = "tar";
        }};

        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            assertFalse("Auto-detect is not set.", provider.isFormatAutoDetection());
            assertEquals("1 format is set", 1, provider.getFormats().length);
            assertEquals("a configured format is set", "tar", provider.getFormats()[0]);
        }
    }

    @Test
    public void testOpenNextNoFile() throws Exception {
        new NonStrictExpectations() {{
            files.nextFile(); result = false;
        }};

        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            assertNull("No file is returned.", provider.openNext());
        }
    }

    @Test
    public void testOpenNextOneFileOneStream(
            @Mocked final ArchiveInputStreamIterator iterator,
            @Mocked final InputStream in) throws Exception {
        final CommonsCompressProvider forPartialMock = new CommonsCompressProvider(task, files);
        new NonStrictExpectations(CommonsCompressProvider.class) {{
            forPartialMock.createInputStreamIterator((InputStream)any); result = iterator;
            files.nextFile(); result = true; result = false;
            task.getFormat(); result = "";
            iterator.hasNext(); result = true; result = false;
            iterator.next(); result = in;
        }};

        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            assertTrue("Return a stream", in == provider.openNext());
            assertNull("No stream found", provider.openNext());
        }

        new Verifications() {{
            files.nextFile(); times = 2;
            iterator.hasNext(); times = 2;
            iterator.next(); times = 1;
        }};

        forPartialMock.close();
    }

    @Test
    public void testOpenNextOneFileOneStreamForFormat(
            @Mocked final ArchiveInputStreamIterator iterator,
            @Mocked final InputStream in) throws Exception {
        final CommonsCompressProvider forPartialMock = new CommonsCompressProvider(task, files);
        new NonStrictExpectations(CommonsCompressProvider.class) {{
            forPartialMock.createInputStreamIterator((String[])any, 0, (InputStream)any); result = iterator;
            files.nextFile(); result = true; result = false;
            task.getFormat(); result = "tar";
            iterator.hasNext(); result = true; result = false;
            iterator.next(); result = in;
        }};

        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            assertTrue("Return a stream", in == provider.openNext());
            assertNull("No stream found", provider.openNext());
        }

        new Verifications() {{
            files.nextFile(); times = 2;
            iterator.hasNext(); times = 2;
            iterator.next(); times = 1;
        }};

        forPartialMock.close();
    }

    @Test
    public void testOpenNextOneFileTwoStreams(@Mocked final ArchiveInputStreamIterator iterator,
            @Mocked final InputStream in1, @Mocked final InputStream in2) throws Exception {
        final CommonsCompressProvider forPartialMock = new CommonsCompressProvider(task, files);
        new NonStrictExpectations(CommonsCompressProvider.class) {{
            forPartialMock.createInputStreamIterator((InputStream)any); result = iterator;
            files.nextFile(); result = true; result = false;
            task.getFormat(); result = "";
            iterator.hasNext(); result = true; result = true; result = false;
            iterator.next(); result = in1; result = in2;
        }};

        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            assertNotNull("Return 1st stream", in1 == provider.openNext());
            assertNotNull("Return 2nd stream", in2 == provider.openNext());
            assertNull("No stream found", provider.openNext());
        }

        new Verifications() {{
            files.nextFile(); times = 2;
            iterator.hasNext(); times = 3;
            iterator.next(); times = 2;
        }};
        
        forPartialMock.close();
    }

    @Test
    public void testOpenNextTwoFilesTwoStreams(
            @Mocked final ArchiveInputStreamIterator iterator1,
            @Mocked final ArchiveInputStreamIterator iterator2,
            @Mocked final InputStream in1,
            @Mocked final InputStream in2) throws Exception {
        final CommonsCompressProvider forPartialMock = new CommonsCompressProvider(task, files);
        new NonStrictExpectations(CommonsCompressProvider.class) {{
            forPartialMock.createInputStreamIterator((InputStream)any); result = iterator1; result = iterator2;
            files.nextFile(); result = true; result = true; result = false;
            task.getFormat(); result = "";
            iterator1.hasNext(); result = true; result = true; result = false;
            iterator1.next(); result = in1; result = in2;
            iterator2.hasNext(); result = true; result = true; result = false;
            iterator2.next(); result = in1; result = in2;
        }};

        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            assertNotNull("Return 1st stream", in1 == provider.openNext());
            assertNotNull("Return 2nd stream", in2 == provider.openNext());
            assertNotNull("Return 3rd stream", in1 == provider.openNext());
            assertNotNull("Return 4th stream", in2 == provider.openNext());
            assertNull("No stream found", provider.openNext());
        }

        new Verifications() {{
            files.nextFile(); times = 3;
            iterator1.hasNext(); times = 3;
            iterator1.next(); times = 2;
            iterator2.hasNext(); times = 3;
            iterator2.next(); times = 2;
        }};
        
        forPartialMock.close();
    }

    @Test
    public void testCreateInputStreamIteratorAutoDetectForArchive() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            Iterator<InputStream> it = provider.createInputStreamIterator(
                    getResourceInputStream("samples.tar"));
            verifyContents(it, "1,foo", "2,bar");
        }
    }

    @Test
    public void testCreateInputStreamIteratorAutDetectForCompressor() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            Iterator<InputStream> it = provider.createInputStreamIterator(
                    getResourceInputStream("sample_1.csv.bz2"));
            verifyContents(it, "1,foo");
        }
    }

    // It looks difficult to detect these solid compression format. So, right now,
    // this doesn't support to use auto detect.
    /*
    @Test
    public void testCreateInputStreamIteratorAutoDetectForSolidCompression() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            Iterator<InputStream> it = provider.createInputStreamIterator(
                    getResourceInputStream("samples.tar.bz2"));
            verifyContents(it, "1,foo", "2,bar");
        }
    }*/

    @Test
    public void testCreateInputStreamIteratorFormatsAndArchive() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            Iterator<InputStream> it = provider.createInputStreamIterator(
                    new String[]{ArchiveStreamFactory.TAR}, 0, getResourceInputStream("samples.tar"));
            verifyContents(it, "1,foo", "2,bar");
        }
    }
    
    @Test
    public void testCreateInputStreamIteratorFormatsAndCompressor() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            Iterator<InputStream> it = provider.createInputStreamIterator(
                    new String[]{CompressorStreamFactory.BZIP2}, 0, getResourceInputStream("sample_1.csv.bz2"));
            verifyContents(it, "1,foo");
        }
    }
    
    @Test
    public void testCreateInputStreamIteratorFormatsAndSolidCompression() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            Iterator<InputStream> it = provider.createInputStreamIterator(
                    new String[]{CompressorStreamFactory.BZIP2, ArchiveStreamFactory.TAR},
                    0, getResourceInputStream("samples.tar.bz2"));
            verifyContents(it, "1,foo", "2,bar");
        }
    }

    @Test
    public void testCreateInputStreamConcatenatedGZ() throws Exception {
        new NonStrictExpectations() {{
            task.getDecompressConcatenated(); result = true;
        }};

        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            Iterator<InputStream> it = provider.createInputStreamIterator(
                    new String[]{CompressorStreamFactory.GZIP}, 0, getResourceInputStream("concatenated.csv.gz"));
            verifyContents(it, "1,foo\n2,bar");
        }
    }

    @Test
    public void testCreateInputStreamConcatenatedGZip() throws Exception {
        new NonStrictExpectations() {{
            task.getDecompressConcatenated(); result = true;
        }};

        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            Iterator<InputStream> it = provider.createInputStreamIterator(
                    CommonsCompressUtil.toFormats("gzip"), 0, getResourceInputStream("concatenated.csv.gz"));
            verifyContents(it, "1,foo\n2,bar");
        }
    }

    @Test
    public void testCreateInputStreamConcatenatedBZip2() throws Exception {
        new NonStrictExpectations() {{
            task.getDecompressConcatenated(); result = true;
        }};

        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            Iterator<InputStream> it = provider.createInputStreamIterator(
                    new String[]{CompressorStreamFactory.BZIP2}, 0, getResourceInputStream("concatenated.csv.bz2"));
            verifyContents(it, "1,foo\n2,bar");
        }
    }

    @Test
    public void testCreateInputStreamConcatenatedBZ2() throws Exception {
        new NonStrictExpectations() {{
            task.getDecompressConcatenated(); result = true;
        }};

        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            Iterator<InputStream> it = provider.createInputStreamIterator(
                    CommonsCompressUtil.toFormats("bz2"), 0, getResourceInputStream("concatenated.csv.bz2"));
            verifyContents(it, "1,foo\n2,bar");
        }
    }

    @Test
    public void testClose() throws Exception {
        CommonsCompressProvider provider = new CommonsCompressProvider(task, files);
        provider.close();

        new Verifications() {{
            files.close(); times = 1;
        }};
    }

    @Test
    public void testCreateArchiveInputStreamFormat() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            ArchiveInputStream in = provider.createArchiveInputStream("tar",
                    getResourceInputStream("samples.tar"));
            assertNotNull("Verify an instance is returned.", in);
        }
    }

    // NOTE: This may need to handle instead of null. But, it means that there is no entry.
    // So, it may be ok...
    public void testCreateArchiveInputStreamWrongFormat() throws IOException, ArchiveException {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            ArchiveInputStream in = provider.createArchiveInputStream("zip",
                    getResourceInputStream("samples.tar"));
            assertNull("Verify a wrong format null in this testcase.", in.getNextEntry());
        }
    }

    @Test(expected=ArchiveException.class)
    public void testCreateArchiveInputStreamFormatNotFound() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            provider.createArchiveInputStream("non-existing-format",
                    getResourceInputStream("samples.tar"));
        }
    }

    @Test
    public void testCreateArchiveInputStreamAutoDetect() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            ArchiveInputStream in = provider.createArchiveInputStream("",
                    getResourceInputStream("samples.tar"));
            assertNotNull("Verify an instance is returned.", in);
        }
    }

    @Test(expected=IOException.class)
    public void testCreateArchiveInputStreamAutoDetectIOException() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            provider.createArchiveInputStream("", getResourceInputStream("sample_1.csv"));
        }
    }

    @Test
    public void testCreateCompressorInputStreamFormat() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            CompressorInputStream in = provider.createCompressorInputStream("bzip2",
                    getResourceInputStream("samples.tar.bz2"));
            assertNotNull("Verify an instance is returned.", in);
        }
    }

    @Test(expected=Exception.class)
    public void testCreateCompressorInputStreamWrongFormat() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            provider.createCompressorInputStream("bzip2",
                    getResourceInputStream("samples.tar"));
        }
    }

    @Test(expected=CompressorException.class)
    public void testCreateCompressorInputStreamFormatNotFound() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            provider.createCompressorInputStream("no-existing-format",
                    getResourceInputStream("samples.tar.bz2"));
        }
    }

    @Test
    public void testCreateCompressorInputStreamAutoDetect() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            CompressorInputStream in = provider.createCompressorInputStream("",
                    getResourceInputStream("samples.tar.bz2"));
            assertNotNull("Verify an instance is returned.", in);
        }
    }

    @Test(expected=IOException.class)
    public void testCreateCompressorInputStreamAutoDetectIOException() throws Exception {
        try (CommonsCompressProvider provider = new CommonsCompressProvider(task, files)) {
            provider.createCompressorInputStream("", getResourceInputStream("sample_1.csv"));
        }
    }

    private void verifyContents(Iterator<InputStream> it, String ...expected) throws IOException {
        for (String text : expected) {
            assertTrue("There is a contents.", it.hasNext());
            assertEquals("Verify the contents.", text, toString(it.next()).trim());
        }
        assertFalse("There is no contents.", it.hasNext());
    }

    private String toString(InputStream in) throws IOException {
        return new String(toByteArray(in));
    }

    private byte[] toByteArray(InputStream in) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        byte[] buff = new byte[1024];
        int len = in.read(buff);
        while (len != -1) {
            bout.write(buff, 0, len);
            len = in.read(buff);
        }
        return bout.toByteArray();
    }

    private InputStream getResourceInputStream(String resource) throws IOException {
        InputStream in = getClass().getResourceAsStream(resource);
        try {
            return new ByteArrayInputStream(toByteArray(in));
        } finally {
            in.close();
        }
    }
}

package org.embulk.decoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.List;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.config.DataSource;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.spi.Buffer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.DecoderPlugin;
import org.embulk.spi.FileInput;
import org.junit.Assert;
import org.junit.Test;

public class TestCommonsCompressDecoderPlugin
{
    private static final String DEFAULT_FORMAT_CONFIG = "\"\"";

    @Mocked
    CommonsCompressDecoderPlugin.PluginTask task;

    @Mocked
    TaskSource taskSource;

    @Test
    public void testPluginTask() throws Exception {
        Method method = CommonsCompressDecoderPlugin.PluginTask.class.getMethod("getFormat");
        Config config = method.getAnnotation(Config.class);
        ConfigDefault configDefault = method.getAnnotation(ConfigDefault.class);
        
        Assert.assertEquals("Verify the config name.", "format", config.value());
        Assert.assertEquals("Verify the default config value.", DEFAULT_FORMAT_CONFIG, configDefault.value());
    }

    @Test
    public void testPluginTaskGetDecompressConcatenated() throws Exception {
        Method method = CommonsCompressDecoderPlugin.PluginTask.class.getMethod("getDecompressConcatenated");
        Config config = method.getAnnotation(Config.class);
        ConfigDefault configDefault = method.getAnnotation(ConfigDefault.class);

        Assert.assertEquals("Verify the config name.", "decompress_concatenated", config.value());
        Assert.assertEquals("Verify the default config value.", "true", configDefault.value());
    }

    @Test
    public void testTransaction(@Mocked final ConfigSource config, @Mocked final DecoderPlugin.Control control)
    {
        new Expectations() {{
            task.toTaskSource(); result = taskSource;
        }};

        CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
        plugin.transaction(config, control);

        new Verifications() {{
            task.toTaskSource(); times = 1;
            control.run(taskSource); times = 1;
        }};
    }

    @Test
    public void testOpen(@Mocked final FileInput input)
    {
        CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
        Assert.assertNotNull("Verify a value is returned.", plugin.open(taskSource, input));
    }

    // sample_0 contains only a directory.
    @Test
    public void testOpenForNoFile(@Mocked final FileInput input) throws Exception
    {
        new Expectations() {{
            task.getFormat(); result = "tar";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("sample_0.tar");
        }};

        CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);

        Assert.assertFalse("Verify there is no file.", archiveFileInput.nextFile());
        archiveFileInput.close();

        new Verifications() {{
            input.nextFile(); times = 2;
            input.close(); times = 1;
        }};
    }

    // sample_1.tar contains 1 csv file.
    @Test
    public void testOpenForOneFile(@Mocked final FileInput input) throws Exception
    {
        new Expectations() {{
            task.getFormat(); result = "tar";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("sample_1.tar");
        }};

        CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);

        verifyContents(archiveFileInput, "1,foo");

        new Verifications() {{
            input.nextFile(); times = 2;
            input.close(); times = 1;
        }};
    }

    // input.nextFile() returns true
    // samples.zip/sample_1.csv (1st)
    // samples.zip/sample_2.csv (2nd)
    @Test
    public void testOpenForTwoFiles(@Mocked final FileInput input) throws Exception
    {
        new Expectations() {{
            task.getFormat(); result = "tar";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("samples.tar");
        }};

        CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);

        verifyContents(archiveFileInput, "1,foo", "2,bar");

        new Verifications() {{
            input.nextFile(); times = 2;
            input.close(); times = 1;
        }};
    }

    @Test
    public void testOpenArchiveFormatAutoDetect(@Mocked final FileInput input) throws Exception
    {
        new Expectations() {{
            task.getFormat(); result = "";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("sample_1.tar");
        }};

        CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);

        verifyContents(archiveFileInput, "1,foo");

        new Verifications() {{
            input.nextFile(); times = 2;
            input.close(); times = 1;
        }};
    }

    @Test(expected=RuntimeException.class)
    public void testOpenAutoDetectFailed(@Mocked final FileInput input) throws Exception
    {
        new Expectations() {{
            task.getFormat(); result = "";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("sample_1.csv"); // This is not an archive.
        }};

        CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);
        archiveFileInput.nextFile();
    }

    @Test(expected=RuntimeException.class)
    public void testOpenExplicitConfigFailed(@Mocked final FileInput input) throws Exception
    {
        new Expectations() {{
            task.getFormat(); result = "tar";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("samples.zip"); // This is not tar file.
        }};

        CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);
        archiveFileInput.nextFile();
    }

    @Test
    public void testOpenForGeneratedArchives() throws Exception
    {
        String[] testFormats = new String[]{
                ArchiveStreamFactory.AR,
                // ArchiveStreamFactory.ARJ, // ArchiveException: Archiver: arj not found.
                ArchiveStreamFactory.CPIO,
                // ArchiveStreamFactory.DUMP, // ArchiveException: Archiver: dump not found.
                ArchiveStreamFactory.JAR,
                // ArchiveStreamFactory.SEVEN_Z, // StreamingNotSupportedException: The 7z doesn't support streaming.
                ArchiveStreamFactory.TAR,
                ArchiveStreamFactory.ZIP,
        };

        for (String format : testFormats) {
            TaskSource mockTaskSource = new MockTaskSource(format);
            FileInput mockInput = new MockFileInput(
                    getInputStreamAsBuffer(
                            getArchiveInputStream(format, "sample_1.csv", "sample_2.csv")));
            CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
            FileInput archiveFileInput = plugin.open(mockTaskSource, mockInput);
            verifyContents(archiveFileInput, "1,foo", "2,bar");
        }
    }

    @Test
    public void testOpenForGeneratedCompression() throws Exception
    {
        String[] testFormats = new String[]{
                CompressorStreamFactory.BZIP2,
                CompressorStreamFactory.DEFLATE,
                CompressorStreamFactory.GZIP,
                // CompressorStreamFactory.LZMA, // CompressorException: Compressor: lzma not found.
                // CompressorStreamFactory.PACK200, // Failed to generate compressed file.
                // CompressorStreamFactory.SNAPPY_FRAMED, // CompressorException: Compressor: snappy-framed not found.
                // CompressorStreamFactory.SNAPPY_RAW, // CompressorException: Compressor: snappy-raw not found.
                // CompressorStreamFactory.XZ, // ClassNotFoundException: org.tukaani.xz.FilterOptions
                // CompressorStreamFactory.Z, // CompressorException: Compressor: z not found.
        };

        for (String format : testFormats) {
            TaskSource mockTaskSource = new MockTaskSource(format);
            FileInput mockInput = new MockFileInput(
                    getInputStreamAsBuffer(
                            getCompressorInputStream(format, "sample_1.csv")));
            CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
            FileInput archiveFileInput = plugin.open(mockTaskSource, mockInput);
            verifyContents(archiveFileInput, "1,foo");
        }
    }

    @Test
    public void testOpenForTGZFormat(@Mocked final FileInput input) throws Exception
    {
        new Expectations() {{
            task.getFormat(); result = "tgz";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("samples.tgz");
        }};

        CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);

        verifyContents(archiveFileInput, "1,foo", "2,bar");

        new Verifications() {{
            input.nextFile(); times = 2;
            input.close(); times = 1;
        }};
    }

    @Test
    public void testOpenForTarGZFormat(@Mocked final FileInput input) throws Exception
    {
        new Expectations() {{
            task.getFormat(); result = "tar.gz";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("samples.tar.gz");
        }};

        CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);

        verifyContents(archiveFileInput, "1,foo", "2,bar");

        new Verifications() {{
            input.nextFile(); times = 2;
            input.close(); times = 1;
        }};
    }

    // NOTE: This may generate a warn relates to log4j...I am not sure why it is generated.
    @Test
    public void testOpenForTarBZ2Format(@Mocked final FileInput input) throws Exception
    {
        new Expectations() {{
            task.getFormat(); result = "tar.bz2";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("samples.tar.bz2");
        }};

        CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);
        
        verifyContents(archiveFileInput, "1,foo", "2,bar");

        new Verifications() {{
            input.nextFile(); times = 2;
            input.close(); times = 1;
        }};
    }

    // This works well. So, uncompress for tar.Z looks to work.
    @Test
    public void testOpenForTarZFormat(@Mocked final FileInput input) throws Exception
    {
        new Expectations() {{
            task.getFormat(); result = "tar.Z";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("samples.tar.Z");
        }};

        CommonsCompressDecoderPlugin plugin = newMockedCommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);
        
        verifyContents(archiveFileInput, "1,foo", "2,bar");

        new Verifications() {{
            input.nextFile(); times = 2;
            input.close(); times = 1;
        }};
    }

    private Buffer getInputStreamAsBuffer(InputStream in) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        byte[] buff = new byte[1024];
        int len = in.read(buff);
        while (len != -1) {
          bout.write(buff, 0, len);
          len = in.read(buff);
        }
        in.close();
        return new MockBuffer(bout.toByteArray());
    }

    private Buffer getResourceAsBuffer(String resource) throws IOException {
        return getInputStreamAsBuffer(getClass().getResourceAsStream(resource));
    }


    private String readFileInput(FileInput input) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        Buffer buffer = input.poll();
        while (buffer != null) {
            bout.write(buffer.array(), buffer.offset(), buffer.limit());
            buffer = input.poll();
        }
        return bout.toString().trim();
    }
    
    private void verifyContents(FileInput input, String ...contents) throws IOException {
        for (String expected : contents) {
            Assert.assertTrue("Verify a file can be read." + expected, input.nextFile());
            String text = readFileInput(input);
            Assert.assertEquals("Verify a file read correctly. text:" + text, expected, text);
        }
        Assert.assertFalse("Verify there is no file.", input.nextFile());
        input.close();
    }
    
    private InputStream getArchiveInputStream(String format, String ...resourceFiles)
            throws ArchiveException, URISyntaxException, IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ArchiveStreamFactory factory = new ArchiveStreamFactory();
        ArchiveOutputStream aout = factory.createArchiveOutputStream(format, bout);

        for (String resource : resourceFiles) {
            File f = new File(getClass().getResource(resource).toURI());
            ArchiveEntry entry = aout.createArchiveEntry(f, resource);
            aout.putArchiveEntry(entry);
            IOUtils.copy(new FileInputStream(f),aout);
            aout.closeArchiveEntry();
        }
        aout.finish();
        aout.close();

        return new ByteArrayInputStream(bout.toByteArray());
    }

    private InputStream getCompressorInputStream(String format, String resource)
            throws CompressorException, URISyntaxException, IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        CompressorStreamFactory factory = new CompressorStreamFactory();
        CompressorOutputStream aout = factory.createCompressorOutputStream(format, bout);

        File f = new File(getClass().getResource(resource).toURI());
        IOUtils.copy(new FileInputStream(f), aout);
        aout.close();

        return new ByteArrayInputStream(bout.toByteArray());
    }

    private class MockTaskSource implements TaskSource {

        MockTaskSource(String format) {
        }

        @Override
        public <E> E get(Class<E> arg0, String arg1) {
            return null;
        }

        @Override
        public <E> E get(Class<E> arg0, String arg1, E arg2) {
            return null;
        }

        @Override
        public List<String> getAttributeNames() {
            return null;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public TaskSource deepCopy() {
            return null;
        }

        @Override
        public TaskSource getNested(String arg0) {
            return null;
        }

        @Override
        public TaskSource getNestedOrSetEmpty(String arg0) {
            return null;
        }

        @Override
        public TaskSource merge(DataSource arg0) {
            return null;
        }

        @Override
        public TaskSource set(String arg0, Object arg1) {
            return null;
        }

        @Override
        public TaskSource setAll(DataSource arg0) {
            return null;
        }

        @Override
        public TaskSource setNested(String arg0, DataSource arg1) {
            return null;
        }

        public boolean has(String arg0) {
            return false;
        }

        public TaskSource getNestedOrGetEmpty(String arg0) {
            return null;
        }

        @Override
        public TaskSource remove(String arg0) {
            return null;
        }
        
    }

    private class MockFileInput implements FileInput {
        Buffer buffer;

        MockFileInput(Buffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void close() {
        }

        @Override
        public boolean nextFile() {
            return buffer != null;
        }

        @Override
        public Buffer poll() {
            if (buffer != null) {
                Buffer ret = buffer;
                buffer = null;
                return ret;
            } else {
                return null;
            }
        }
    }

    private class MockBufferAllocator implements BufferAllocator {
        @Override
        public Buffer allocate() {
            return allocate(8192);
        }

        @Override
        public Buffer allocate(int size) {
            return new MockBuffer(new byte[size], 0, size);
        }
    }

    private class MockBuffer extends Buffer {
        private int offset;
        private int limit;
        private byte[] bytes;

        MockBuffer(byte[] bytes) {
            this(bytes, 0, bytes.length);
        }

        MockBuffer(byte[] bytes, int index, int length) {
            offset = index;
            limit = length;
            this.bytes = bytes;
        }

        @Override
        public byte[] array() {
            return bytes;
        }

        @Override
        public int offset() {
            return offset;
        }

        @Override
        public Buffer offset(int offset) {
            this.offset = offset;
            return this;
        }

        @Override
        public int limit() {
            return limit;
        }

        @Override
        public Buffer limit(int limit) {
            this.limit = limit;
            return this;
        }

        @Override
        public int capacity() {
            return bytes.length;
        }

        @Override
        public void setBytes(int index, byte[] source, int sourceIndex, int length) {
            System.arraycopy(source, sourceIndex, bytes, index, length);
        }

        @Override
        public void setBytes(int index, Buffer source, int sourceIndex, int length) {
            byte[] sourceBytes = new byte[source.capacity()];
            source.getBytes(0, sourceBytes, 0, sourceBytes.length);
            setBytes(index, sourceBytes, sourceIndex, length);
        }

        @Override
        public void getBytes(int index, byte[] dest, int destIndex, int length) {
            if (bytes != null && dest != null) {
                System.arraycopy(bytes, offset + index, dest, destIndex, length);
            }
        }

        @Override
        public void getBytes(int index, Buffer dest, int destIndex, int length) {
            dest.setBytes(destIndex, bytes, offset + index, length);
        }

        @Override
        public void release() {
            this.bytes = null;
        }
    }

    private CommonsCompressDecoderPlugin newMockedCommonsCompressDecoderPlugin() {
        return new CommonsCompressDecoderPlugin() {
            BufferAllocator getBufferAllocator() {
                return new MockBufferAllocator();
            }

            PluginTask getTask(ConfigSource configSource) {
                return task;
            }

            PluginTask getTask(TaskSource taskSource) {
                return task;
            }
        };
    }
}

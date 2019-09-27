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
import java.util.Map.Entry;

import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.DataSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.DecoderPlugin;
import org.embulk.spi.FileInput;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


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
        new NonStrictExpectations() {{
            config.loadConfig(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
            task.dump(); result = taskSource;
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
        plugin.transaction(config, control);

        new Verifications() {{
            control.run(taskSource);
        }};
    }

    @Test
    public void testOpen(@Mocked final FileInput input)
    {
        new NonStrictExpectations() {{
            taskSource.loadTask(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();

        Assert.assertNotNull("Verify a value is returned.", plugin.open(taskSource, input));
    }

    // sample_0 contains only a directory.
    @Test
    public void testOpenForNoFile(@Mocked final FileInput input) throws Exception
    {
        new NonStrictExpectations() {{
            taskSource.loadTask(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
            task.getFormat(); result = "tar";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("sample_0.tar");
            task.getBufferAllocator(); result = newBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
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
        new NonStrictExpectations() {{
            taskSource.loadTask(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
            task.getFormat(); result = "tar";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("sample_1.tar");
            task.getBufferAllocator(); result = newBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
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
        new NonStrictExpectations() {{
            taskSource.loadTask(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
            task.getFormat(); result = "tar";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("samples.tar");
            task.getBufferAllocator(); result = newBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);

        verifyContents(archiveFileInput, "1,foo", "2,bar");

        new Verifications() {{
            input.nextFile(); times = 2;
            input.close(); times = 1;
        }};
    }

    // input.nextFile() returns true
    // samples.zip/sample_1.csv (1st)
    // samples.zip/sample_2.csv (2nd)
    // input.nextFile() returns true
    // samples.zip/sample_1.csv (3rd)
    // samples.zip/sample_2.csv (4th)
    @Test
    public void testOpenForFourFiles(@Mocked final FileInput input) throws Exception
    {
        new NonStrictExpectations() {{
            taskSource.loadTask(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
            task.getFormat(); result = "zip";
            input.nextFile(); result = true; result = true; result = false; // two files.
            input.poll(); result = getResourceAsBuffer("samples.zip"); result = getResourceAsBuffer("samples.zip");
            task.getBufferAllocator(); result = newBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);

        verifyContents(archiveFileInput, "1,foo", "2,bar", "1,foo", "2,bar");

        new Verifications() {{
            input.nextFile(); times = 3;
            input.close(); times = 1;
        }};
    }

    @Test
    public void testOpenArchiveFormatAutoDetect(@Mocked final FileInput input) throws Exception
    {
        new NonStrictExpectations() {{
            taskSource.loadTask(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
            task.getFormat(); result = "";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("sample_1.tar");
            task.getBufferAllocator(); result = newBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
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
        new NonStrictExpectations() {{
            taskSource.loadTask(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
            task.getFormat(); result = "";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("sample_1.csv"); // This is not an archive.
            task.getBufferAllocator(); result = newBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);
        archiveFileInput.nextFile();
    }
    @Test(expected=RuntimeException.class)
    public void testOpenExplicitConfigFailed(@Mocked final FileInput input) throws Exception
    {
        new NonStrictExpectations() {{
            taskSource.loadTask(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
            task.getFormat(); result = "tar";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("samples.zip"); // This is not tar file.
            task.getBufferAllocator(); result = newBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
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
            CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
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
            CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
            FileInput archiveFileInput = plugin.open(mockTaskSource, mockInput);
            verifyContents(archiveFileInput, "1,foo");
        }
    }

    @Test
    public void testOpenForTGZFormat(@Mocked final FileInput input) throws Exception
    {
        new NonStrictExpectations() {{
            taskSource.loadTask(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
            task.getFormat(); result = "tgz";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("samples.tgz");
            task.getBufferAllocator(); result = newBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
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
        new NonStrictExpectations() {{
            taskSource.loadTask(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
            task.getFormat(); result = "tar.gz";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("samples.tar.gz");
            task.getBufferAllocator(); result = newBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
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
        new NonStrictExpectations() {{
            taskSource.loadTask(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
            task.getFormat(); result = "tar.bz2";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("samples.tar.bz2");
            task.getBufferAllocator(); result = newBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
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
        new NonStrictExpectations() {{
            taskSource.loadTask(CommonsCompressDecoderPlugin.PluginTask.class); result = task;
            task.getFormat(); result = "tar.Z";
            input.nextFile(); result = true; result = false;
            input.poll(); result = getResourceAsBuffer("samples.tar.Z");
            task.getBufferAllocator(); result = newBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
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
        return Buffer.wrap(bout.toByteArray());
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
            Assert.assertTrue("Verify a file can be read.", input.nextFile());
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

    private BufferAllocator newBufferAllocator() {
        return new MockBufferAllocator();
    }

    private class MockTaskSource implements TaskSource {
        private final String format;

        MockTaskSource(String format) {
            this.format = format;
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
        public Iterable<Entry<String, JsonNode>> getAttributes() {
            return null;
        }

        @Override
        public ObjectNode getObjectNode() {
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
        public <T> T loadTask(Class<T> clazz) {
            if (CommonsCompressDecoderPlugin.PluginTask.class.equals(clazz)) {
                return clazz.cast(new MockPluginTask(format));
            }
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

    private class MockPluginTask implements CommonsCompressDecoderPlugin.PluginTask {
        private final String format;
        private final boolean decompressConcatenated;
        private final String matchName;
        private final boolean passUncompressFile;

        MockPluginTask(String format) {
            this.format = format;
            this.decompressConcatenated = true;
            this.matchName = "";
            this.passUncompressFile = false;
        }

        @Override
        public TaskSource dump() {
            return null;
        }

        @Override
        public void validate() {
        }

        @Override
        public String getFormat() {
            return format;
        }

        @Override
        public boolean getDecompressConcatenated() {
            return decompressConcatenated;
        }

        @Override
        public String getMatchName() {
            return matchName;
        }

        @Override
        public BufferAllocator getBufferAllocator() {
            return newBufferAllocator();
        }

        @Override
        public boolean getPassUncompressFile() {
            return passUncompressFile;
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
            return Buffer.allocate(size);
        }
    }
}

package org.embulk.decoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;

import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.exec.PooledBufferAllocator;
import org.embulk.spi.Buffer;
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
            task.getBufferAllocator(); result = new PooledBufferAllocator();
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
            task.getBufferAllocator(); result = new PooledBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);
        String text;

        Assert.assertTrue("Verify 1st file can be read.", archiveFileInput.nextFile());
        text = readFileInput(archiveFileInput);
        Assert.assertEquals("Verify 1st file read correctly.", "1,foo", text);

        Assert.assertFalse("Verify there is no file.", archiveFileInput.nextFile());
        archiveFileInput.close();

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
            task.getBufferAllocator(); result = new PooledBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);
        String text;

        Assert.assertTrue("Verify 1st file can be read.", archiveFileInput.nextFile());
        text = readFileInput(archiveFileInput);
        Assert.assertEquals("Verify 1st file read correctly.", "1,foo", text);

        Assert.assertTrue("Verify 2nd file can be read.", archiveFileInput.nextFile());
        text = readFileInput(archiveFileInput);
        Assert.assertEquals("Verify 2nd file read correctly.", "2,bar", text);

        Assert.assertFalse("Verify there is no file.", archiveFileInput.nextFile());
        archiveFileInput.close();

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
            task.getBufferAllocator(); result = new PooledBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);
        String text;

        Assert.assertTrue("Verify the 1st file can be read.", archiveFileInput.nextFile());
        text = readFileInput(archiveFileInput);
        Assert.assertEquals("Verify the 1st file read correctly.", "1,foo", text);

        Assert.assertTrue("Verify the 2nd file can be read.", archiveFileInput.nextFile());
        text = readFileInput(archiveFileInput);
        Assert.assertEquals("Verify the 2nd file read correctly.", "2,bar", text);

        Assert.assertTrue("Verify the 3rd file can be read.", archiveFileInput.nextFile());
        text = readFileInput(archiveFileInput);
        Assert.assertEquals("Verify the 3rd file read correctly.", "1,foo", text);

        Assert.assertTrue("Verify the 4th file can be read.", archiveFileInput.nextFile());
        text = readFileInput(archiveFileInput);
        Assert.assertEquals("Verify the 4th file read correctly.", "2,bar", text);

        Assert.assertFalse("Verify there is no file.", archiveFileInput.nextFile());
        archiveFileInput.close();

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
            task.getBufferAllocator(); result = new PooledBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);
        String text;

        Assert.assertTrue("Verify 1st file can be read.", archiveFileInput.nextFile());
        text = readFileInput(archiveFileInput);
        Assert.assertEquals("Verify 1st file read correctly.", "1,foo", text);

        Assert.assertFalse("Verify there is no file.", archiveFileInput.nextFile());
        archiveFileInput.close();

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
            task.getBufferAllocator(); result = new PooledBufferAllocator();
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
            task.getBufferAllocator(); result = new PooledBufferAllocator();
        }};

        CommonsCompressDecoderPlugin plugin = new CommonsCompressDecoderPlugin();
        FileInput archiveFileInput = plugin.open(taskSource, input);
        archiveFileInput.nextFile();
    }

    private Buffer getResourceAsBuffer(String resource) throws IOException {
        InputStream in = getClass().getResourceAsStream(resource);
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


    private String readFileInput(FileInput input) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        Buffer buffer = input.poll();
        while (buffer != null) {
            bout.write(buffer.array(), buffer.offset(), buffer.limit());
            buffer = input.poll();
        }
        return bout.toString().trim();
    }
}

package org.embulk.decoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import mockit.Mocked;
import mockit.NonStrictExpectations;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.junit.Test;

public class TestArchiveInputStreamIterator {

    @Test
    public void testHasNextForNoEntry(@Mocked final ArchiveInputStream ain) throws Exception {
        new NonStrictExpectations() {{
            ain.getNextEntry(); result = null;
        }};
        ArchiveInputStreamIterator it = new ArchiveInputStreamIterator(ain);
        assertFalse("Verify there is no next item.", it.hasNext());
        assertNull("Verify there is no stream.", it.next());
    }

    @Test
    public void testHasNextForOneEntry(@Mocked final ArchiveInputStream ain, @Mocked final ArchiveEntry entry) throws Exception {
        new NonStrictExpectations() {{
            ain.getNextEntry(); result = entry; result = null;
            entry.isDirectory(); result = false;
        }};
        ArchiveInputStreamIterator it = new ArchiveInputStreamIterator(ain);
        assertTrue("Verify there is a item.", it.hasNext());
        assertEquals("Verify ArchiveInputStream is return.", (InputStream)ain, it.next());
        assertFalse("Verify there is no next item.", it.hasNext());
        assertNull("Verify there is no stream.", it.next());
    }

    @Test
    public void testHasNextForTwoEntries(@Mocked final ArchiveInputStream ain, @Mocked final ArchiveEntry entry) throws Exception {
        new NonStrictExpectations() {{
            ain.getNextEntry(); result = entry; result = entry; result = null;
            entry.isDirectory(); result = false;
        }};
        ArchiveInputStreamIterator it = new ArchiveInputStreamIterator(ain);
        assertTrue("Verify there is 1st item.", it.hasNext());
        assertEquals("Verify ArchiveInputStream is return.", (InputStream)ain, it.next());
        assertTrue("Verify there is 2nd item.", it.hasNext());
        assertEquals("Verify ArchiveInputStream is return.", (InputStream)ain, it.next());
        assertFalse("Verify there is no next item.", it.hasNext());
        assertNull("Verify there is no stream.", it.next());
    }

    @Test
    public void testHasNextForDirectory(@Mocked final ArchiveInputStream ain, @Mocked final ArchiveEntry entry) throws Exception {
        new NonStrictExpectations() {{
            ain.getNextEntry(); result = entry; result = entry; result = null;
            entry.isDirectory(); result = false; result = true;
        }};
        ArchiveInputStreamIterator it = new ArchiveInputStreamIterator(ain);
        assertTrue("Verify there is 1st item.", it.hasNext());
        assertEquals("Verify ArchiveInputStream is return.", (InputStream)ain, it.next());
        assertFalse("Verify there is no next item.", it.hasNext());
        assertNull("Verify there is no stream.", it.next());
    }

    @Test
    public void testArchiveFile() throws Exception {
        InputStream in = getClass().getResourceAsStream("samples.tar");
        ArchiveInputStream ain = new ArchiveStreamFactory().createArchiveInputStream(in);
        ArchiveInputStreamIterator it = new ArchiveInputStreamIterator(ain);
        assertTrue("Verify there is 1st item.", it.hasNext());
        assertEquals("Verify ArchiveInputStream is return.", "1,foo", readContents(it.next()));
        assertTrue("Verify there is 2nd item.", it.hasNext());
        assertEquals("Verify ArchiveInputStream is return.", "2,bar", readContents(it.next()));
        assertFalse("Verify there is no next item.", it.hasNext());
        assertNull("Verify there is no stream.", it.next());

        // Verify calling after no data.
        assertFalse("Verify there is no next item.", it.hasNext());
        assertNull("Verify there is no stream.", it.next());
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testRemove(@Mocked final ArchiveInputStream ain) {
        ArchiveInputStreamIterator it = new ArchiveInputStreamIterator(ain);
        it.remove();
    }

    private String readContents(InputStream in) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        byte[] buff = new byte[128];
        int len = in.read(buff);
        while (len != -1) {
            bout.write(buff, 0, len);
            len = in.read(buff);
        }
        return bout.toString().trim();
    }
}

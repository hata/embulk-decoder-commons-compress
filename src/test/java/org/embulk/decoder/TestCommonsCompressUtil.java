package org.embulk.decoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.junit.Test;

public class TestCommonsCompressUtil {

    @Test
    public void testIsArchiveFormat() {
        for (String format : CommonsCompressUtil.archiveFormats) {
            assertTrue("Archive formats should return true.", CommonsCompressUtil.isArchiveFormat(format));
        }
        for (String format : CommonsCompressUtil.compressorFormats) {
            assertFalse("Compressor formats should return false.", CommonsCompressUtil.isArchiveFormat(format));
        }
        assertFalse("Auto-detect should return false.", CommonsCompressUtil.isArchiveFormat(""));
    }

    @Test
    public void testIsCompressorFormat() {
        for (String format : CommonsCompressUtil.archiveFormats) {
            assertFalse("Archive formats should return false.", CommonsCompressUtil.isCompressorFormat(format));
        }
        for (String format : CommonsCompressUtil.compressorFormats) {
            assertTrue("Compressor formats should return true.", CommonsCompressUtil.isCompressorFormat(format));
        }
        assertFalse("Auto-detect should return false.", CommonsCompressUtil.isArchiveFormat(""));
    }

    @Test
    public void testIsAutoDetect() {
        for (String format : CommonsCompressUtil.archiveFormats) {
            assertFalse("Archive formats should return false.", CommonsCompressUtil.isAutoDetect(format));
        }
        for (String format : CommonsCompressUtil.compressorFormats) {
            assertFalse("Compressor formats should return false.", CommonsCompressUtil.isAutoDetect(format));
        }
        assertTrue("Verify auto detect format.", CommonsCompressUtil.isAutoDetect(null));
        assertTrue("Verify auto detect format.", CommonsCompressUtil.isAutoDetect(""));
    }

    @Test
    public void testToFormatsForAutoDetect() {
        assertNull("null returns null.", CommonsCompressUtil.toFormats(null));
        assertNull("zero length string returns null.", CommonsCompressUtil.toFormats(""));
    }
    
    @Test
    public void testToFormatsForSingleFormat() {
        String format = ArchiveStreamFactory.TAR;
        String[] formats = CommonsCompressUtil.toFormats(format);
        assertEquals("a single format returns 1 length array.", 1, formats.length);
        assertEquals("a single format returns tar.", ArchiveStreamFactory.TAR, formats[0]);
    }

    @Test
    public void testToFormatsForSolidCompressionFormat() {
        String format = "tgz";
        String[] formats = CommonsCompressUtil.toFormats(format);
        assertEquals("solid compresson format returns 2 length array.", 2, formats.length);
        assertEquals("solid compresson format returns gzip for 1st element.", CompressorStreamFactory.GZIP, formats[0]);
        assertEquals("solid compresson format returns tar for 2st element.", ArchiveStreamFactory.TAR, formats[1]);
    }

    @Test
    public void testToFormatsForMultipleFormats() {
        String format = ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.BZIP2;
        String[] formats = CommonsCompressUtil.toFormats(format);
        assertEquals("Two format text returns 2 length array.", 2, formats.length);
        assertEquals("Two format text returns bzip2 for 1st element.", CompressorStreamFactory.BZIP2, formats[0]);
        assertEquals("Two format text returns tar for 1st element.", ArchiveStreamFactory.TAR, formats[1]);
    }

}

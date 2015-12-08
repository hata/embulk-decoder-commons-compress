package org.embulk.filter;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.Test;

public class TestIntegration {
    static final String TEST_DIR = System.getProperty("embulk.integrationtest.dir");
    private static final String[] SAMPLE_SRC_FILES = {"header.csv", "sample_1.csv", "sample_2.csv"};
    private static final String[] SAMPLE_1_SRC_FILES = {"header.csv", "sample_1.csv"};

    private static String getTestFile(String name) {
        return TEST_DIR + File.separator + name;
    }

    @Test
    public void testArchiveFormatZip() throws Exception {
        assertEquals("Verify input and output contents are identical.",
                getChecksumFromFiles(SAMPLE_SRC_FILES),
                getChecksumFromFiles("result_zip_000.00.csv"));
    }

    @Test
    public void testArchiveFormatAr() throws Exception {
        assertEquals("Verify input and output contents are identical.",
                getChecksumFromFiles(SAMPLE_SRC_FILES),
                getChecksumFromFiles("result_ar_000.00.csv"));
    }

    @Test
    public void testArchiveFormatTar() throws Exception {
        assertEquals("Verify input and output contents are identical.",
                getChecksumFromFiles(SAMPLE_SRC_FILES),
                getChecksumFromFiles("result_tar_000.00.csv"));
    }

    @Test
    public void testCompressionFormatBzip2() throws Exception {
        assertEquals("Verify input and output contents are identical.",
                getChecksumFromFiles(SAMPLE_1_SRC_FILES),
                getChecksumFromFiles("result_bz2_000.00.csv"));
    }

    @Test
    public void testCompressionFormatGzip() throws Exception {
        assertEquals("Verify input and output contents are identical.",
                getChecksumFromFiles(SAMPLE_1_SRC_FILES),
                getChecksumFromFiles("result_gz_000.00.csv"));
    }

    @Test
    public void testSolidCompressionFormatTgz() throws Exception {
        assertEquals("Verify input and output contents are identical.",
                getChecksumFromFiles(SAMPLE_SRC_FILES),
                getChecksumFromFiles("result_tgz_000.00.csv"));
    }

    @Test
    public void testSolidCompressionFormatTarBz2() throws Exception {
        assertEquals("Verify input and output contents are identical.",
                getChecksumFromFiles(SAMPLE_SRC_FILES),
                getChecksumFromFiles("result_tar.bz2_000.00.csv"));
    }

    @Test
    public void testSolidCompressionFormatTarGz() throws Exception {
        assertEquals("Verify input and output contents are identical.",
                getChecksumFromFiles(SAMPLE_SRC_FILES),
                getChecksumFromFiles("result_tar.gz_000.00.csv"));
    }

    @Test
    public void testSolidCompressionFormatTarZ() throws Exception {
        assertEquals("Verify input and output contents are identical.",
                getChecksumFromFiles(SAMPLE_SRC_FILES),
                getChecksumFromFiles("result_tar.Z_000.00.csv"));
    }

    private long getChecksumFromFiles(String ... files) throws IOException {
        Checksum cksum = new CRC32();

        for (String srcFile : files) {
            try (BufferedReader reader = new BufferedReader(new FileReader(getTestFile(srcFile)))) {
                getChecksum(cksum, reader);
            }
        }
        
        return cksum.getValue();
    }
    
    private long getChecksum(Checksum cksum, BufferedReader reader) throws IOException {
        String line = reader.readLine();
        while (line != null) {
            byte[] lineBuf = line.trim().getBytes();
            if (lineBuf.length > 0) {
                // System.out.println("line:" + new String(lineBuf));
                cksum.update(lineBuf, 0, lineBuf.length);
            }
            line = reader.readLine();
        }
        return cksum.getValue();
    }
}

package org.embulk.decoder;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.embulk.decoder.CommonsCompressDecoderPlugin.PluginTask;
import org.embulk.spi.util.FileInputInputStream;
import org.embulk.spi.util.InputStreamFileInput.Provider;

class CommonsCompressProvider implements Provider {
    private static final String AUTO_DETECT_FORMAT = "";

    private final FileInputInputStream files;
    private final boolean formatAutoDetection;
    private Iterator<InputStream> inputStreamIterator;
    private String[] formats;
    private final boolean decompressConcatenated;

    CommonsCompressProvider(PluginTask task, FileInputInputStream files) {
        this.files = files;
        this.formatAutoDetection = task == null
                || CommonsCompressUtil.isAutoDetect(task.getFormat());
        if (!this.formatAutoDetection) {
            formats = CommonsCompressUtil.toFormats(task.getFormat());
            if (formats == null) {
                throw new RuntimeException("Failed to get a format.");
            }
        }
        this.decompressConcatenated = task == null
            || task.getDecompressConcatenated();
    }

    @Override
    public InputStream openNext() throws IOException {
        while (true) {
            if (inputStreamIterator == null) {
                if (!files.nextFile()) {
                    return null;
                }
                inputStreamIterator = formatAutoDetection ? createInputStreamIterator(files)
                        : createInputStreamIterator(formats, 0, files);
            } else {
                if (inputStreamIterator.hasNext()) {
                    InputStream in = inputStreamIterator.next();
                    if (in == null) {
                        inputStreamIterator = null;
                    } else {
                        return in;
                    }
                } else {
                    inputStreamIterator = null;
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        inputStreamIterator = null;
        if (files != null) {
            files.close();
        }
    }

    boolean isFormatAutoDetection() {
        return formatAutoDetection;
    }
    
    String[] getFormats() {
        return formats;
    }

    Iterator<InputStream> createInputStreamIterator(InputStream in)
            throws IOException {
        // It is required to support mark to detect a file format.
        in = in.markSupported() ? in : new BufferedInputStream(in);
        try {
            return new ArchiveInputStreamIterator(
                    createArchiveInputStream(AUTO_DETECT_FORMAT, in));
        } catch (IOException | ArchiveException e) {
            // ArchiveStreamFactory set mark and reset the stream.
            // So, we can use the same stream to check compressor.
            try {
                return toIterator(createCompressorInputStream(AUTO_DETECT_FORMAT, in));
            } catch (CompressorException e2) {
                throw new IOException("Failed to detect a file format.", e2);
            }
        }
    }

    /**
     * Create iterator to list InputStream for each archived/compressed file.
     * 
     * This can handle like the following formats:
     * 1 archived format which defined in ArchiveStreamFactory(e.g. tar)
     * 1 archived format and 1 compressor format defined in CompressorStreamFactory.(e.g. tar.bz2)
     * 1 compressor format defined in CompressorStreamFactory.(e.g. bz2)
     * (Actually, compressor formats can use two or more times in this code.
     *  But it is not common case.)
     */
    Iterator<InputStream> createInputStreamIterator(String[] inputFormats,
            int pos, InputStream in) throws IOException {
        if (pos >= inputFormats.length) {
            return toIterator(in);
        }

        try {
            String format = inputFormats[pos];
            if (CommonsCompressUtil.isArchiveFormat(format)) {
                return new ArchiveInputStreamIterator(
                        createArchiveInputStream(format, in));
            } else if (CommonsCompressUtil.isCompressorFormat(format)) {
                return createInputStreamIterator(inputFormats, pos + 1,
                        createCompressorInputStream(format, in));
            }
            throw new IOException("Unsupported format is configured. format:"
                    + format);
        } catch (ArchiveException | CompressorException e) {
            throw new IOException(e);
        }
    }

    /**
     * Create a new ArchiveInputStream to read an archive file based on a format
     * parameter.
     * 
     * If format is not set, this method tries to detect file format
     * automatically. In this case, BufferedInputStream is used to wrap
     * FileInputInputStream instance. BufferedInputStream may read a data
     * partially when calling files.nextFile(). However, it doesn't matter
     * because the partial read data should be discarded. And then this method
     * is called again to create a new ArchiveInputStream.
     * 
     * @return a new ArchiveInputStream instance.
     */
    ArchiveInputStream createArchiveInputStream(String format, InputStream in)
            throws IOException, ArchiveException {
        ArchiveStreamFactory factory = new ArchiveStreamFactory();
        if (CommonsCompressUtil.isAutoDetect(format)) {
            in = in.markSupported() ? in : new BufferedInputStream(in);
            try {
                return factory.createArchiveInputStream(in);
            } catch (ArchiveException e) {
                throw new IOException(
                        "Failed to detect a file format. Please try to set a format explicitly.",
                        e);
            }
        } else {
            return factory.createArchiveInputStream(format, in);
        }
    }

    CompressorInputStream createCompressorInputStream(String format,
            InputStream in) throws IOException, CompressorException {
        CompressorStreamFactory factory = new CompressorStreamFactory();
        factory.setDecompressConcatenated(decompressConcatenated);
        if (CommonsCompressUtil.isAutoDetect(format)) {
            in = in.markSupported() ? in : new BufferedInputStream(in);
            try {
                return factory.createCompressorInputStream(in);
            } catch (CompressorException e) {
                throw new IOException(
                        "Failed to detect a file format. Please try to set a format explicitly.",
                        e);
            }
        } else {
            return factory.createCompressorInputStream(format, in);
        }
    }

    private Iterator<InputStream> toIterator(InputStream in) {
        List<InputStream> list = new ArrayList<InputStream>(1);
        list.add(in);
        return list.iterator();
    }
}

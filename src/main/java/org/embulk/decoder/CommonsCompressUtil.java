package org.embulk.decoder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

class CommonsCompressUtil {
    // TODO: It may be better to check performance between Set and array.
    // NOTE: Some file types may not work in an environment because some required
    // libraries are not found.
    static final String[] archiveFormats = {
        ArchiveStreamFactory.AR,
        ArchiveStreamFactory.ARJ,
        ArchiveStreamFactory.CPIO,
        ArchiveStreamFactory.DUMP,
        ArchiveStreamFactory.JAR,
        ArchiveStreamFactory.SEVEN_Z,
        ArchiveStreamFactory.TAR,
        ArchiveStreamFactory.ZIP,
    };

    // Even indexes have both extensions and aliases. And odd indexes are
    // CompressorStreamFactory values.
    static final String[] compressorFormats = {
        CompressorStreamFactory.BZIP2,
        CompressorStreamFactory.DEFLATE,
        CompressorStreamFactory.GZIP,
        CompressorStreamFactory.LZMA,
        CompressorStreamFactory.PACK200,
        CompressorStreamFactory.SNAPPY_FRAMED,
        CompressorStreamFactory.SNAPPY_RAW,
        CompressorStreamFactory.XZ,
        CompressorStreamFactory.Z,
        "bz2", // These values should be handled by normalizeFormats
        "gzip",
    };

    // This table is even indexes have short extensions and odd indexes has
    // split formats for each short extensions.
    private static final String[] solidCompressionFormats = {
        "tgz",      ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.GZIP,
        "tar.gz",   ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.GZIP,
        "tbz",      ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.BZIP2,
        "tbz2",     ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.BZIP2,
        "tb2",      ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.BZIP2,
        "tar.bz2",  ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.BZIP2,
        "taz",      ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.Z,
        "tz",       ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.Z,
        "tar.Z",    ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.Z,
        "tlz",      ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.LZMA,
        "tar.lz",   ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.LZMA,
        "tar.lzma", ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.LZMA,
        "txz",      ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.XZ,
        "tar.xz",   ArchiveStreamFactory.TAR + " " + CompressorStreamFactory.XZ
    };

    static boolean isArchiveFormat(String format) {
        for (String fmt : archiveFormats) {
            if (fmt.equalsIgnoreCase(format)) {
                return true;
            }
        }
        return false;
    }

    static boolean isCompressorFormat(String format) {
        for (String fmt : compressorFormats) {
            if (fmt.equalsIgnoreCase(format)) {
                return true;
            }
        }
        return false;
    }

    static boolean isAutoDetect(String format) {
        return format == null || format.length() == 0;
    }

    /**
     * Split solid compresson formats and reorder to decode the formats
     * based on this order.
     *
     * If format is a single format like "tar", then return
     * new String[]{"tar"}.
     * If format is a solid compresson format like "tgz", then return
     * new String[]{"gzip", "tar"}.
     * If format is "tar bzip2", then return
     * new String[]{"bzip2", "tar"}.
     *
     * @param format contains a file format or some file formats.
     * @return a single format or multi format values.
     * Otherwise, returns null.
     */
    static String[] toFormats(String format) {
        if (isAutoDetect(format)) {
            return null;
        } else if (isArchiveFormat(format) || isCompressorFormat(format)) {
            return normalizeFormats(splitAndReverse(format));
        }

        String[] formats = toSolidCompressionFormats(format);
        if (formats != null) {
            return formats;
        }

        formats = normalizeFormats(splitAndReverse(format));

        for (String s : formats) {
            if (!(isArchiveFormat(s) || isCompressorFormat(s))) {
                return null;
            }
        }

        return formats;
    }

    private static String[] toSolidCompressionFormats(String format) {
        for (int i = 0;i < solidCompressionFormats.length; i+= 2) {
            if (solidCompressionFormats[i].equalsIgnoreCase(format)) {
                return splitAndReverse(solidCompressionFormats[i + 1]);
            }
        }
        return null;
    }

    private static String[] splitAndReverse(String format) {
        List<String> result = new ArrayList<>();
        for (String s : format.split(" ")) {
            if (s.length() > 0) {
                result.add(s);
            }
        }
        Collections.reverse(result);
        return result.toArray(new String[result.size()]);
    }

    private static String[] normalizeFormats(String... formats) {
        if (formats == null || formats.length == 0) {
            return formats;
        }

        for (int i = 0;i < formats.length;i++) {
            if (formats[i].equalsIgnoreCase("gzip")) {
                formats[i] = CompressorStreamFactory.GZIP;
            } else if (formats[i].equalsIgnoreCase("bz2")) {
                formats[i] = CompressorStreamFactory.BZIP2;
            }
        }

        return formats;
    }
}

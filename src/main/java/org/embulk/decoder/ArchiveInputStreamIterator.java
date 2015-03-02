package org.embulk.decoder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;

class ArchiveInputStreamIterator implements Iterator<InputStream> {
    private ArchiveInputStream ain;
    private ArchiveEntry entry;
    private boolean endOfArchive = false;

    ArchiveInputStreamIterator(ArchiveInputStream ain)
    {
        this.ain = ain;
    }

    @Override
    public boolean hasNext() {
        try {
            return checkNext();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream next() {
        try {
            if (checkNext()) {
                entry = null;
            } else {
                return null;
            }

            return ain;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private boolean checkNext() throws IOException {
        if (endOfArchive) {
            return false;
        } else if (entry != null) {
            return true;
        }

        while (true) {
            entry = ain.getNextEntry();
            if (entry == null) {
                endOfArchive = true;
                return false;
            } else if (entry.isDirectory()) {
                continue;
            } else {
                return true;
            }
        }
    }
}

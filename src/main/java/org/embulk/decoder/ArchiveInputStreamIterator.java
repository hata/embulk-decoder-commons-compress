package org.embulk.decoder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;

class ArchiveInputStreamIterator implements Iterator<InputStream> {
    private ArchiveInputStream ain;
    private ArchiveEntry entry;
    private String matchRegex = "";
    private boolean endOfArchive = false;

    ArchiveInputStreamIterator(ArchiveInputStream ain)
    {
        this.ain = ain;
    }

    ArchiveInputStreamIterator(ArchiveInputStream ain, String matchRegex) {
        this.ain = ain;
        this.matchRegex = matchRegex;
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
            } else if (!matchName(entry, matchRegex)){
                continue;
            } else {
                return true;
            }
        }
    }

    private boolean matchName(ArchiveEntry entry, String regex) {
        String name = entry.getName();
        if(regex == null || regex.equals("")){
            return true;
        } else if(name == null) {
            return false;
        } else {
            return name.matches(regex);
        }
    }
}

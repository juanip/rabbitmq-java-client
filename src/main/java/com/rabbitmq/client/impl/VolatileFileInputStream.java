package com.rabbitmq.client.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * InputStream supported by a temporary file
 * The temporary file is deleted when calling close().
 */
public class VolatileFileInputStream extends FileInputStream {

    private File tempFile;

    public VolatileFileInputStream(File tempFile) throws FileNotFoundException {
        super(tempFile);
        this.tempFile = tempFile;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        }
        finally {
            // tries to explicitly delete the temp file here
            tempFile.delete();
        }

    }
}

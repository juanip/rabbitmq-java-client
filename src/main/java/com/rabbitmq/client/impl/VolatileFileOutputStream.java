package com.rabbitmq.client.impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

/**
 * OutputStream supported by a temporary file.
 * It provides a method to get an InputStream from the file.
 * The temporary file is deleted when calling close() unless the method getInputStream() was called before.
 */
public class VolatileFileOutputStream extends OutputStream {

    private static final String TEMP_FILE_PREFIX = "rabbitmq_";
    private static final String TEMP_FILE_SUFFIX = ".tmp";

    private final OutputStream outputStream;
    private File tempFile;

    public VolatileFileOutputStream() throws IOException {
        tempFile = Files.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX).toFile();
        outputStream = new BufferedOutputStream(new FileOutputStream(tempFile));
        tempFile.deleteOnExit();
    }

    @Override
    public void write(int i) throws IOException {
        outputStream.write(i);
    }

    @Override
    public void write(byte[] b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        outputStream.write(b, off, len);
    }

    /**
     * Returns an InputStream using the temporary file as source and closes this instance.
     * Returns null if the instance is already closed.
     * @return the inputStream
     * @throws IOException
     */
    public InputStream getInputStream() throws IOException {
        if (tempFile == null) {
            return null;
        }

        // sets the tempFile to null to avoid deleting it when calling close()
        File file = tempFile;
        tempFile = null;
        close();

        return new VolatileFileInputStream(file);
    }

    @Override
    public void close() throws IOException {
        try {
            outputStream.flush();
            outputStream.close();
        }
        finally {
            // tries to explicitly delete the temp file here if it is still defined
            if (tempFile != null) {
                tempFile.delete();
                tempFile = null;
            }
        }
    }
}

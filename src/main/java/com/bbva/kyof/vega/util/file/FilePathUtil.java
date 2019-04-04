package com.bbva.kyof.vega.util.file;

import lombok.NonNull;

import java.io.File;
import java.io.IOException;

/**
 * File path utility class
 */
public final class FilePathUtil
{
    /** String error when there are not enough privileges */
    private static final String INSUFFICIENT_PRIVILEGES_ERROR = "Insufficient privileges to access path [%s]";

    /** Private constructor to avoid instantiation of utility class */
    private FilePathUtil()
    {
        // Nothing to do
    }

    /**
     * Verify the given file path, checking that is not null, it exists, and is an actual file
     *
     * @param filePath the path to the file to verify
     * @throws IOException exception thrown if the path is not valid
     */
    public static void verifyFilePath(@NonNull final String filePath) throws IOException
    {
        // Create the file over the path
        final File file = new File(filePath);

        // Verify
        checkIfExists(file);
        checkIfIsFile(file);
    }

    /**
     * Verify the given directory path, checking that is not null, it exists, and is an actual directory
     *
     * @param dirPath the path to the directory to verify
     * @throws IOException exception thrown if the path is not valid
     */
    public static void verifyDirPath(@NonNull final String dirPath) throws IOException
    {
        // Create the file over the path
        final File directory = new File(dirPath);

        // Verify
        checkIfExists(directory);
        checkIfIsDir(directory);
    }

    /**
     * Check if the given file exists
     * @param file the file to check against
     * @throws IOException exception thrown if it doesn't exists
     */
    private static void checkIfExists(final File file) throws IOException
    {
        try
        {
            if (!file.exists())
            {
                throw new IOException(String.format("Cannot find the provided path [%s]", file.getPath()));
            }
        }
        catch (final SecurityException e)
        {
            throw new IOException(String.format(INSUFFICIENT_PRIVILEGES_ERROR, file.getPath()), e);
        }
    }

    /**
     * Check if the given file is an actual file
     * @param file the file to check against
     * @throws IOException exception thrown if is not a file
     */
    private static void checkIfIsFile(final File file) throws IOException
    {
        try
        {
            if (!file.isFile())
            {
                throw new IOException(String.format("The provided file path [%s] is not a file", file.getPath()));
            }
        }
        catch (final SecurityException e)
        {
            throw new IOException(String.format(INSUFFICIENT_PRIVILEGES_ERROR, file.getPath()), e);
        }
    }

    /**
     * Check if the given file is a directory
     * @param dir the directory to check against
     * @throws IOException exception thrown if is not a directory
     */
    private static void checkIfIsDir(final File dir) throws IOException
    {
        try
        {
            if (!dir.isDirectory())
            {
                throw new IOException(String.format("The provided directory path [%s] is not a directory", dir.getPath()));
            }
        }
        catch (final SecurityException e)
        {
            throw new IOException(String.format(INSUFFICIENT_PRIVILEGES_ERROR, dir.getPath()), e);
        }
    }
}

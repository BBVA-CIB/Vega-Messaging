package com.bbva.kyof.vega.util.file;

import com.bbva.kyof.vega.autodiscovery.daemon.CommandLineParserTest;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * Created by cnebrera on 13/10/2016.
 */
public class FilePathUtilTest
{
    private static final String DUMMY_FILE = CommandLineParserTest.class.getClassLoader().getResource("pathUtilTest/dummy.txt").getPath();
    private static final String DUMMY_DIR = DUMMY_FILE.replace("/dummy.txt", "");

    @Test
    public void testConstructor() throws Exception
    {
        Constructor<?>[] cons = FilePathUtil.class.getDeclaredConstructors();
        cons[0].setAccessible(true);
        cons[0].newInstance((Object[]) null);
    }

    @Test
    public void verifyFilePath() throws Exception
    {
        FilePathUtil.verifyFilePath(DUMMY_FILE);
    }

    @Test(expected = IOException.class)
    public void verifyFilePathFailsDontExists() throws Exception
    {
        FilePathUtil.verifyFilePath(DUMMY_DIR + "/nonExistingFile.txt");
    }

    @Test(expected = IOException.class)
    public void verifyFilePathFailsNotFile() throws Exception
    {
        FilePathUtil.verifyFilePath(DUMMY_DIR);
    }

    @Test(expected = NullPointerException.class)
    public void verifyFilePathNull() throws Exception
    {
        FilePathUtil.verifyFilePath(null);
    }

    @Test
    public void verifyDirPath() throws Exception
    {
        FilePathUtil.verifyDirPath(DUMMY_DIR);
    }

    @Test(expected = IOException.class)
    public void verifyDirPathNonExists() throws Exception
    {
        FilePathUtil.verifyFilePath(DUMMY_DIR + "/nonExistingFile.txt");
    }

    @Test(expected = IOException.class)
    public void verifyDirPathNonDir() throws Exception
    {
        FilePathUtil.verifyDirPath(DUMMY_FILE);
    }

    @Test(expected = NullPointerException.class)
    public void verifyDirPathNull() throws Exception
    {
        FilePathUtil.verifyDirPath(null);
    }
}
package com.s3.client;

import static junit.framework.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;

import com.s3.config.Config;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class, classes = Config.class)
public class AwsS3WrapperImplIT
{
    @Autowired
    private AwsS3WrapperImpl s3Wrapper;

    @Test
    public void testS3Upload() throws IOException
    {
        InputStream fstream = this.getClass().getResourceAsStream("/test.txt");
        String eTag = s3Wrapper.uploadFile("admin.server.qa", "index1.txt", IOUtils.toByteArray(fstream));
        fstream.close();
        assertNotNull(eTag);
    }

    @Test
    public void testS3LargeFileUpload() throws IOException
    {
        InputStream fstream = this.getClass().getResourceAsStream("/test.txt");
        String eTag = s3Wrapper.uploadFileAsynchronously("admin.server.qa", "index.txt", IOUtils.toByteArray(fstream));
        fstream.close();
        assertNotNull(eTag);
    }

    @Test
    public void testS3LargeFileUploadByFilePath() throws IOException
    {
        String eTag = s3Wrapper.uploadFile("admin.server.qa", "file:///Users/mohit/test.txt");
        assertNotNull(eTag);
    }

}

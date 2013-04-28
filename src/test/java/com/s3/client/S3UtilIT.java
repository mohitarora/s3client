package com.s3.client;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Random;

import com.google.common.primitives.Bytes;
import org.junit.Test;

public class S3UtilIT
{
    @Test
    public void given16MByteArray_whenFileBytesAreSplitInto3_thenTheSplitIsCorrect()
    {
        byte[] byteArray = randomByteData(16);

        int maximumNumberOfParts = S3Util.getMaximumNumberOfParts(byteArray);

        List<byte[]> fileParts = S3Util.breakByteArrayIntoParts(byteArray, maximumNumberOfParts);

        assertThat(fileParts.get(0).length + fileParts.get(1).length + fileParts.get(2).length,
            equalTo(byteArray.length));

        byte[] unmultiplexed = Bytes.concat(fileParts.get(0), fileParts.get(1), fileParts.get(2));

        assertThat(byteArray, equalTo(unmultiplexed));
    }

    private byte[] randomByteData(int mb)
    {
        byte[] randomBytes = new byte[mb * 1024 * 1024];
        new Random().nextBytes(randomBytes);
        return randomBytes;
    }
}

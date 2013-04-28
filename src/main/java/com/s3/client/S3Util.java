package com.s3.client;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;

public final class S3Util
{
    private S3Util()
    {
    }

    /**
     * This method will get the number of parts S3 Upload process needs to be broken. This method will make sure that we
     * don’t have parts below this 5 MB limit
     *
     * @param byteArray
     *
     * @return
     */
    public static int getMaximumNumberOfParts(byte[] byteArray)
    {
        int fiveMB = 5242880;
        int numberOfParts = byteArray.length / fiveMB; // 5*1024*1024
        if (numberOfParts == 0)
        {
            return 1;
        }
        return numberOfParts;
    }

    /**
     * This method will break byte array into parts so that S3 upload can run in parallel for these parts.
     *
     * @param byteArray
     * @param maxNumberOfParts
     *
     * @return
     */
    public static List<byte[]> breakByteArrayIntoParts(byte[] byteArray, int maxNumberOfParts)
    {
        List<byte[]> parts = Lists.newArrayListWithCapacity(maxNumberOfParts);
        int fullSize = byteArray.length;
        long dimensionOfPart = fullSize / maxNumberOfParts;
        for (int i = 0; i < maxNumberOfParts; i++)
        {
            int previousSplitPoint = (int) (dimensionOfPart * i);
            int splitPoint = (int) (dimensionOfPart * (i + 1));
            if (i == (maxNumberOfParts - 1))
            {
                splitPoint = fullSize;
            }
            byte[] partBytes = Arrays.copyOfRange(byteArray, previousSplitPoint, splitPoint);
            parts.add(partBytes);
        }

        return parts;
    }

    /**
     * Create Payload out of file parts that needs to be uploaded to S3
     *
     * @param fileParts
     *
     * @return
     */
    public static List<Payload> createPayloadsFromParts(Iterable<byte[]> fileParts)
    {
        List<Payload> payloads = Lists.newArrayList();
        for (byte[] filePart : fileParts)
        {
            byte[] partMd5Bytes = Hashing.md5().hashBytes(filePart).asBytes();
            Payload partPayload = Payloads.newByteArrayPayload(filePart);
            partPayload.getContentMetadata().setContentLength((long) filePart.length);
            partPayload.getContentMetadata().setContentMD5(partMd5Bytes);
            payloads.add(partPayload);
        }
        return payloads;
    }


    /**
     * Create Payload out of file bytes that needs to be uploaded to S3
     *
     * @param file
     *
     * @return
     */
    public static Payload createPayload(byte[] file)
    {
        byte[] md5Bytes = Hashing.md5().hashBytes(file).asBytes();
        Payload payload = Payloads.newByteArrayPayload(file);
        payload.getContentMetadata().setContentLength((long) file.length);
        payload.getContentMetadata().setContentMD5(md5Bytes);
        return payload;
    }
}

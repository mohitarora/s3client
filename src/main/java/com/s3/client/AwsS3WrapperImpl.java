package com.s3.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.IOUtils;
import org.jclouds.ContextBuilder;
import org.jclouds.aws.s3.AWSS3AsyncClient;
import org.jclouds.aws.s3.AWSS3Client;
import org.jclouds.aws.s3.blobstore.AWSS3BlobStoreContext;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.Payload;
import org.jclouds.rest.RestContext;
import org.jclouds.s3.domain.ObjectMetadata;
import org.jclouds.s3.domain.ObjectMetadataBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

@Component
public class AwsS3WrapperImpl implements InitializingBean
{
    @Autowired
    private ResourceLoader resourceLoader;

    private String accessKey;

    private String secretKey;

    private static final Function<ListenableFuture<String>, String> GET_ETAG_FROM_OPERATION =
        new Function<ListenableFuture<String>, String>()
        {
            public String apply(ListenableFuture<String> ongoingOperation)
            {
                try
                {
                    return ongoingOperation.get();
                }
                catch (InterruptedException e)
                {
                    throw new IllegalStateException(e);
                }
                catch (ExecutionException e)
                {
                    throw new IllegalStateException(e);
                }
            }
        };

    @Override
    public void afterPropertiesSet() throws Exception
    {
        if (accessKey == null)
        {
            throw new IllegalArgumentException("Access key must be provided");
        }
        if (secretKey == null)
        {
            throw new IllegalArgumentException("Secret key must be provided");
        }
    }

    @Value("${aws.accessKey}")
    public void setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
    }

    @Value("${aws.secretKey}")
    public void setSecretKey(String secretKey)
    {
        this.secretKey = secretKey;
    }

    /**
     * Method to Upload files to S3. This method should be used for small files and synchronous upload.
     *
     * @param bucketName
     * @param fileName
     * @param file
     */
    public String uploadFile(String bucketName, String fileName, byte[] file)
    {
        BlobStoreContext context = ContextBuilder.newBuilder("aws-s3").credentials(accessKey, secretKey)
            .buildView(AWSS3BlobStoreContext.class);
        try
        {
            BlobStore blobStore = context.getBlobStore();
            Blob blob = blobStore.blobBuilder(fileName).
                payload(S3Util.createPayload(file)).build();
            String etag = blobStore.putBlob(bucketName, blob, PutOptions.Builder.multipart());
            return etag;
        }
        finally
        {
            context.close();
        }
    }

    /**
     * Method to Upload files to S3. This method should be used for large files and asynchronous upload. This method
     * will break the file into multiple chunks and will upload the chunks in parallel to S3.
     *
     * @param bucketName
     * @param fileName
     * @param file
     */
    public String uploadFileAsynchronously(String bucketName, String fileName, byte[] file)
    {
        BlobStoreContext context = ContextBuilder.newBuilder("aws-s3").
            credentials(accessKey, secretKey).buildView(AWSS3BlobStoreContext.class);
        try
        {
            RestContext<AWSS3Client, AWSS3AsyncClient> providerContext = context.unwrap();
            AWSS3AsyncClient s3AsyncClient = providerContext.getAsyncApi();

            // The first step in the Upload operation is to initiate the process. This request to S3 must contain the
            // standard HTTP headers – the Content-MD5 header in particular needs to be computed. This is the md5
            // hash of  the entire byte array, not of the parts yet.

            byte[] md5FileBytes = Hashing.md5().hashBytes(file).asBytes();

            ObjectMetadata metadata = ObjectMetadataBuilder.create().key(fileName).contentMD5(md5FileBytes).build();
            //This is blocking because upload id is required to move to next step
            String uploadId = s3AsyncClient.initiateMultipartUpload(bucketName, metadata).get();

            int maxParts = S3Util.getMaximumNumberOfParts(file);
            List<byte[]> filePartsAsByteArrays = S3Util.breakByteArrayIntoParts(file, maxParts);

            List<Payload> payloads = S3Util.createPayloadsFromParts(filePartsAsByteArrays);
            //The next step is uploading the parts. Our goal here is to send these requests in parallel
            List<ListenableFuture<String>> ongoingOperations = Lists.newArrayList();
            for (int partNumber = 0; partNumber < filePartsAsByteArrays.size(); partNumber++)
            {
                ListenableFuture<String> future =
                    s3AsyncClient.uploadPart(bucketName, fileName, partNumber + 1, uploadId, payloads.get(partNumber));
                ongoingOperations.add(future);
            }

            //After all of the upload part requests have been submitted, we need to wait for their responses so that we
            // can collect the individual ETag value of each part
            List<String> etagsOfParts = Lists.transform(ongoingOperations, GET_ETAG_FROM_OPERATION);

            //The final step of the upload process is completing the multipart operation. The S3 API requires the
            //responses from the previous parts upload as a Map, which we can now easily create from the list of ETags
            // that we obtained above

            Map<Integer, String> parts = Maps.newHashMap();
            for (int i = 0; i < etagsOfParts.size(); i++)
            {
                parts.put(i + 1, etagsOfParts.get(i));
            }

            //And finally, send the complete request. Again a blocking call
            String etag = s3AsyncClient.completeMultipartUpload(bucketName, fileName, uploadId, parts).get();

            return etag;
        }
        catch (InterruptedException e)
        {
            throw new IllegalStateException(e);
        }
        catch (ExecutionException e)
        {
            throw new IllegalStateException(e);
        }
        finally
        {
            context.close();
        }
    }

    /**
     * This method will upload the file to provided S3 Bucket
     *
     * @param bucketName
     * @param filePath
     *
     * @return
     */
    public String uploadFile(String bucketName, String filePath)
    {
        InputStream fileInputStream = null;
        try
        {
            Resource file = resourceLoader.getResource(filePath);
            if (file.exists())
            {
                return uploadFile(bucketName, file.getFilename(), IOUtils.toByteArray(file.getInputStream()));
            }
            else
            {
                throw new IllegalArgumentException("No File exists at path " + filePath);
            }
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }
        finally
        {
            IOUtils.closeQuietly(fileInputStream);
        }
    }
}

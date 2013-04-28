This module provides classes to uploaded files to Amazon S3 using JClouds Library.

Main Class: AwsS3WrapperImpl

Methods:

--public String uploadFile(String bucketName, String fileName, byte[] file)

--public String uploadFile(String bucketName, String filePath)

Above methods will upload files to S3 synchronously. Any of the above method can be used based on file inputs you have.

--public String uploadFileAsynchronously(String bucketName, String fileName, byte[] file)

This method should be used for large files. This methods will break the file into chunks and will upload all the
chunks in parallel to S3.
# Apache JClouds BlobStore implementation to access pCloud

## Motivation

I am a owner of both a QNAP® NAS and a pCloud® lifetime account. So it was obvious to use pCloud as backup target for QNAP Hybrid Sync Backup®.
Since pCloud is not directly supported as backup target I tried WebDav. 
Unfortunately the pCloud WebDav implementation is both unreliable and slow compared to its native SDK so daily backups with only a few changes took half a day. 
But fortunately QNAP Hybrid Sync Backup also supports S3 compatible clouds, so I searched for possible solutions and found [S3Proxy](https://github.com/gaul/s3proxy), which acts as S3-compatible gateway for various storage backends.
Since [S3Proxy](https://github.com/gaul/s3proxy) uses [Apache jClouds](https://jclouds.apache.org/) as backend, I developed a custom BlobStore for [Apache jClouds](https://jclouds.apache.org/) using the Java Api from [pCloud Java SDK](https://github.com/pCloud/pcloud-sdk-java).

## Content 
Java application containing both a BlobStore implementation for [Apache jClouds](https://jclouds.apache.org/) in the package `com.github.stefanrichterhuber.pCloudForjClouds` and a small main application under `com.github.stefanrichterhuber.s3proxy` connecting this BlobStore with S3Proxy.
The pCloud oauth token is transfered as the identity and credential (both containing the identical token) of S3 user accessing the proxy. This way the proxy has build-in authentication support and there is no need to store pCloud credentials on the proxy in any way.
A pCloud oauth token can be generated using the [pCloud web api](https://docs.pcloud.com/methods/oauth_2.0/authorize.html).

## Example 

```java
     S3Proxy s3Proxy = S3Proxy.builder() //
          .endpoint(URI.create("http://127.0.0.1:8080")) //
          .awsAuthentication(AuthenticationType.AWS_V2_OR_V4, "dummy", "dummy") // Authentication here is ignored
          .build();

     Properties properties = new Properties();
     properties.setProperty(PCloudConstants.PROPERTY_BASEDIR, "/S3"); // Base directory within the pCloud account containing all containers. Should exist
		
     s3Proxy.setBlobStoreLocator(new DynamicPCloudBlobStoreLocator(properties)); // Enables dynamic authentication of pCloud
     s3Proxy.start();
     while (!s3Proxy.getState().equals(AbstractLifeCycle.STARTED)) {
          Thread.sleep(1);
     }

     // AWS client expects MD5 hash while pcloud delivers sha hashes, so disable MD5 validation
     System.setProperty("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true");
     System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
 
     AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
          .withPathStyleAccessEnabled(true) // S3Proxy supports virtual host style, but its far easier to simply use path style access
          .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials([pcloudToken], [pcloudToken]))) // Add pcloud token 
          .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:8080", // URL of the S3 Proxy
               Regions.US_EAST_1.getName()))
          .build();
 ```

## Deployment
Upon building the project with maven the `maven-jar-plugin` is used to generate an executable jar, which just needs the generate `libs` folder and the main `pcloud-s3proxy*.jar` to run.

```cmd
java -jar pcloud-s3proxy.jar -e 0.0.0.0:8080
```

or use the attached dockerfile to build a docker image.

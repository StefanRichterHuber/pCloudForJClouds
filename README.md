![Build](https://github.com/StefanRichterHuber/pCloudForJClouds/actions/workflows/maven.yml/badge.svg)

# Apache JClouds BlobStore implementation to access pCloud

## Motivation

I am a owner of both a QNAP� NAS and a pCloud� lifetime account. So it was obvious to use pCloud as backup target for QNAP Hybrid Sync Backup�.
Since pCloud is not directly supported as backup target I tried WebDav. 
Unfortunately the pCloud WebDav implementation is both unreliable and slow compared to its native SDK so daily backups with only a few changes took half a day. 
But fortunately QNAP Hybrid Sync Backup also supports S3 compatible clouds, so I searched for possible solutions and found [S3Proxy](https://github.com/gaul/s3proxy), which acts as S3-compatible gateway for various storage backends.
Since [S3Proxy](https://github.com/gaul/s3proxy) uses [Apache jClouds](https://jclouds.apache.org/) as backend, I developed a custom BlobStore for [Apache jClouds](https://jclouds.apache.org/) using the Java Api from [pCloud Java SDK](https://github.com/pCloud/pcloud-sdk-java).

## Content 
Java application containing both a BlobStore implementation for [Apache jClouds](https://jclouds.apache.org/) in the package `com.github.stefanrichterhuber.pCloudForjClouds` and a small main application under `com.github.stefanrichterhuber.s3proxy` connecting this BlobStore with S3Proxy.
The pCloud oauth token is transfered  as the identity and credential (both containing the identical token) of S3 user accessing the proxy. This way the proxy has built-in authentication support and there is no need to store pCloud credentials in the proxy in any way. The pCloud API gateway (either `api.pcloud.com` or `eapi.pcloud.com` for European customers) is automatically determined. Custom metadata and hashes of the file a stored in seperate metadata files, this is necessary because S3 API needs MD5 hashes to verify content, but pCloud Java API does not provide this hash. Therefore the MD5 hash is calculated during file upload and stored in a separate file for the next metadata queries. If no metadata file is present or the target file was changed outside the proxy, the checksum is transparently calculated on fetching the metadata (by downloading the file and calculate the checksum), but this is very expensive operation! Unfortunately one can not use [Checksumfile operation](https://docs.pcloud.com/methods/file/checksumfile.html) here, because its results depend on the location of the pCloud store, and does not give MD5 for European customers.
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
 
     AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
          .withPathStyleAccessEnabled(true) // S3Proxy supports virtual host style, but its far easier to simply use path style access
          .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials([pcloudToken], [pcloudToken]))) // Add pcloud token 
          .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:8080", // URL of the S3 Proxy
          // Region does not matter
               Regions.US_EAST_1.getName()))
          .build();
 ```

## Deployment

The application needs an redis instance to store custom metadata and to improve backend performance by storing pCloud file ids. 
It is even possible to store the redis log in pCloud to keep all necessary data in the cloud using the following docker compose file.

```yaml
version: '3.3'

volumes:
  pcloud-cache:

services:
    # rclone instance 
    rclone:
      image: rclone/rclone:latest
      volumes:
          - ./rclone.conf:/config/rclone/rclone.conf # Mount a rclone config file (created by rclone config) into the container
          - pcloud-cache:/rclone-cache # rclone cache persisted as volume
          - ./data:/data:shared
          - /etc/passwd:/etc/passwd:ro
          - /etc/group:/etc/group:ro
          #- /etc/fuse.conf:/etc/fuse.conf:ro
      devices:
        - /dev/fuse:/dev/fuse:rwm
      cap_add:
        - SYS_ADMIN
      security_opt:
        - apparmor:unconfined
      container_name: rclone
      command: 'mount [rclone-config-name]:[folder-within-pCloud-to-store-redis-dump] /data --allow-other --allow-non-empty  --vfs-cache-mode full --cache-dir=/rclone-cache'

    # redis instance to store metadata
    redis:
      image: redis:latest
      depends_on:
        - rclone
      container_name: redis
      devices:
        - /dev/fuse:/dev/fuse:rwm
      cap_add:
        - SYS_ADMIN
      security_opt:
        - apparmor:unconfined
      volumes:
        - ./data:/data:shared #Folder shared with rclone instance. The redis dumps are stored here
        #- /etc/fuse.conf:/etc/fuse.conf:ro
      command: '--save 60 1 --loglevel warning' # Create an redis dump every 60s if at least one changes happened

    # Proxy build, configured to use local redis instance.
    s3proxy:
      depends_on:
        - redis
      image: s3proxy:latest
      container_name: s3proxy
      ports:
        - 8080:8080
      command: '--redis redis://redis:6379'
      labels:

```



Upon building the project with maven, the `maven-jar-plugin` is used to generate an executable jar, which just needs the generated `libs` folder and the main `pcloud-s3proxy*.jar` to run.

```cmd
java -jar pcloud-s3proxy.jar -e 0.0.0.0:8080 -r redis://redis:6379
```

## Limitations
**Use this program at your own risk! There is no guarantee it transfers your files reliably.**


## Results
This S3 proxy is at least two times faster than web dav access (tested with several file sizes). But more important, it is far more stable and works perfectly as backup target for QNAP Hybrid Sync Backup�.

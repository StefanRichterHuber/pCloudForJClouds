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

## Example of embedded deployment

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

The application needs an redis instance to cache custom metadata and to improve backend performance by caching pCloud file ids. The redis cache should be persistent, otherwise the app has to download all metadata files on startup, each time the cache is restarted. To authenticate use the pCloud oauth token for both, client id and client secret when configuring the S3 client.

Upon building the project with maven, the `maven-jar-plugin` is used to generate an executable jar, which just needs the generated `libs` folder and the main `pcloud-s3proxy*.jar` to run.

```cmd
java -jar pcloud-s3proxy.jar -e 0.0.0.0:8080 -r redis://redis:6379
```

To simplify deployment of both proxy and cache, build a Docker image suitable for your target system (both AMD64 or ARM64 was tested) and use docker compose to deploy it. 

```yaml
version: '3.3'

volumes:
  redis-cache:

services:
    redis:
      image: redis:latest
      container_name: redis
      volumes:
        - redis-cache:/data
      command: '--save 60 1 --loglevel warning' 

    s3proxy:
      depends_on:
        - redis
      image: s3proxy:latest
      container_name: s3proxy
      ports:
        - 7543:8080
      command: '--redis redis://redis:6379'
```


## Configuration


| Description | Default value | Command line argument | Environment variable |
| -----------  | ------------- | --------------------  | -------------------  |
| S3 endpoint (S3 server). Should be set to 0.0.0.0:8080 to be reachable from outside (already done in docker file)  | `127:0.0.1:8080` | `-e`, `--endpoint` | - |
| S3 data folder. Folder within pcloud where the blobs are stored | `/S3` | `-b`,`--basedir` | `JCLOUDS_PCLOUD_BASEDIR` |
| S3 metadata folder. Folder within pcloud where the metadata is stored | `/S3-metadata`| `-m`, `--metadatadir` | `JCLOUDS_PCLOUD_USERMETADATA_FOLDER` |
| Redis connnect string |  `redis://redis:6379` | `-r`, `--redis` | `JCLOUDS_PCLOUD_REDIS` |
| Interval (in min) to synchronize redis metadata cache with metadata files at pCloud. Negative values mean no sync (not recommended), 0 means only one sync at startup. Depending on the number of files stored, this can be a very expensive operation. Therefore sync is done incrementally, so only the files changed / added / deleted since the last sync are loaded. It is recommended to use a persistent cache to reduce the load of loading metadata into the cache.  | `0` | - | `JCLOUDS_PCLOUD_USERMETADATA_SYNCHRONIZE_INTERVAL` |
| **Expert-only:** Interval (in min) to check for orphaned metadata entries. Sometimes, when testing or when manually deleting files in the S3 data folder, the corresponding metadata files remain and need to be removed. A negative value  means no scan for orphaned metadata, a value of `0` means the scan is just done once, 5 minutes after start. Depending on the number of files stored, this can be a very expensive operation! | `-1` | - | `JCLOUDS_PCLOUD_USERMETADATA_SANITIZE_INTERVAL` |
| Verbose logging output. Either with repeatable flag `-v` (e.g. `-v`, `-vv`, `-vvv`) or by setting explicit environment variable (possible values `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`) | `ERROR` | `--verbose`, `-v`, `-vv`, `-vvv` | `JCLOUDS_PCLOUD_VERBOSITY` |

## Limitations

**Use this program at your own risk! The program is successfully in use within a local prod environment. But you have to test yourself, if it transfers the files reliable**


## Results

This S3 proxy is at least two times faster than web dav access (tested with several file sizes). But more important, it is far more stable and works perfectly as backup target for QNAP Hybrid Sync Backup�. For optimal performance enable versioning of the files, which leads to bigger chunks to be uploaded. Otherwise each small file is uploaded on its own, which gives a terrible performance. Also adjust compression to the capabilities of your NAS, upload rates might collapse if the NAS is busy with high compression.

package eu.stefanhuber.jclouds.pcloud;

import java.net.URI;
import java.util.Properties;

import org.jclouds.apis.internal.BaseApiMetadata;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.rest.internal.BaseHttpApiMetadata;

import eu.stefanhuber.jclouds.pcloud.config.PCloudBlobStoreContextModule;

public class PCloudApiMetadata extends BaseApiMetadata {

   @Override
   public Builder toBuilder() {
      return new Builder().fromApiMetadata(this);
   }

   public PCloudApiMetadata() {
      super(new Builder());
   }

   protected PCloudApiMetadata(Builder builder) {
      super(builder);
   }

   @Override
   public Properties getDefaultProperties() {
      Properties properties = BaseHttpApiMetadata.defaultProperties();
      return properties;
   }

   public static class Builder extends BaseApiMetadata.Builder<Builder> {

      protected Builder() {
         id("pcloud")
         .name("PCloud-based BlobStore")
         .identityName("Unused")
         .defaultEndpoint("api.pcloud.com")
         .defaultIdentity("Unused")
         .version("1")
         .documentation(URI.create("http://www.jclouds.org/documentation/userguide/blobstore-guide"))
         .defaultProperties(PCloudApiMetadata.defaultProperties())
         .view(BlobStoreContext.class)
         .defaultModule(PCloudBlobStoreContextModule.class);
      }

      @Override
      public PCloudApiMetadata build() {
         return new PCloudApiMetadata(this);
      }

      @Override
      protected Builder self() {
         return this;
      }
   }
}

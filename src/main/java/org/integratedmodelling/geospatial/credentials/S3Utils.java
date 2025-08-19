package org.integratedmodelling.geospatial.credentials;

import com.github.davidmoten.aws.lw.client.Client;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Optional;

public class S3Utils {
    public static Client buildS3Client(String accessKey, String secretKey, Optional<String> region) {
        if (region.isPresent()) {
            return Client.s3().region(region.get()).accessKey(accessKey).secretKey(secretKey).build();
        }
        return Client.s3().regionNone().accessKey(accessKey).secretKey(secretKey).build();
    }

    public static InputStream readS3Object(Client client, String bucket, String key) {
        byte[] contentBytes = client.path(bucket, key).responseAsBytes();
        return new ByteArrayInputStream(contentBytes);
    }
}

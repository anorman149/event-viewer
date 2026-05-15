package org.eventviewer.s3.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import org.eventviewer.s3.HiveKeyBuilder;
import org.eventviewer.s3.S3Client;
import org.eventviewer.s3.S3ClientImpl;
import org.eventviewer.s3.S3Properties;
import org.eventviewer.s3.ZstdCodec;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;
import java.time.Duration;

@AutoConfiguration
@EnableConfigurationProperties(S3Properties.class)
@ConditionalOnProperty(name = "s3.bucket")
public class S3AutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public software.amazon.awssdk.services.s3.S3Client awsS3Client(S3Properties props) {
        SdkHttpClient httpClient = ApacheHttpClient.builder()
                .maxConnections(props.getConnectionPoolSize())
                .socketTimeout(Duration.ofMillis(props.getRequestTimeoutMs()))
                .build();

        S3ClientBuilder builder = software.amazon.awssdk.services.s3.S3Client.builder()
                .region(Region.of(props.getRegion()))
                .httpClient(httpClient);

        if (props.getEndpointOverride() != null && !props.getEndpointOverride().isBlank()) {
            builder.endpointOverride(URI.create(props.getEndpointOverride()))
                   .forcePathStyle(true)
                   .credentialsProvider(StaticCredentialsProvider.create(
                           AwsBasicCredentials.create("test", "test")));
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }

        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean
    public S3Client s3Client(software.amazon.awssdk.services.s3.S3Client awsS3Client,
                              S3Properties props,
                              MeterRegistry registry) {
        return new S3ClientImpl(awsS3Client, props, registry);
    }

    @Bean
    public S3BucketInitializer s3BucketInitializer(software.amazon.awssdk.services.s3.S3Client awsS3Client,
                                                    S3Properties props) {
        return new S3BucketInitializer(awsS3Client, props);
    }

    @Bean
    @ConditionalOnMissingBean
    public ZstdCodec zstdCodec() {
        return new ZstdCodec();
    }

    @Bean
    @ConditionalOnMissingBean
    public HiveKeyBuilder hiveKeyBuilder(S3Properties props) {
        String podName = System.getenv().getOrDefault("MY_POD_NAME", "local-pod");
        return new HiveKeyBuilder(props.getPrefix(), podName);
    }
}

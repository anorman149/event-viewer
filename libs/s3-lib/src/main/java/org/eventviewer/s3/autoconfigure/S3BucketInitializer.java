package org.eventviewer.s3.autoconfigure;

import org.eventviewer.s3.S3Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.LifecycleExpiration;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationRequest;

import java.util.List;

public class S3BucketInitializer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(S3BucketInitializer.class);
    private static final int EXPIRY_DAYS = 5;

    private final S3Client awsS3;
    private final S3Properties props;
    private volatile boolean running = false;

    public S3BucketInitializer(S3Client awsS3, S3Properties props) {
        this.awsS3 = awsS3;
        this.props = props;
    }

    @Override
    public void start() {
        ensureBucketExists();
        applyLifecycleRule();
        running = true;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE + 100;
    }

    private void ensureBucketExists() {
        try {
            awsS3.headBucket(req -> req.bucket(props.getBucket()));
            log.debug("S3 bucket '{}' already exists", props.getBucket());
        } catch (NoSuchBucketException e) {
            try {
                awsS3.createBucket(req -> req.bucket(props.getBucket()));
                log.info("Created S3 bucket '{}'", props.getBucket());
            } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException ignored) {
                log.debug("S3 bucket '{}' created concurrently", props.getBucket());
            }
        }
    }

    private void applyLifecycleRule() {
        LifecycleRule rule = LifecycleRule.builder()
                .id("event-viewer-" + EXPIRY_DAYS + "-day-expiry")
                .status(ExpirationStatus.ENABLED)
                .filter(LifecycleRuleFilter.builder()
                        .prefix(props.getPrefix() + "/")
                        .build())
                .expiration(LifecycleExpiration.builder()
                        .days(EXPIRY_DAYS)
                        .build())
                .build();

        awsS3.putBucketLifecycleConfiguration(PutBucketLifecycleConfigurationRequest.builder()
                .bucket(props.getBucket())
                .lifecycleConfiguration(lc -> lc.rules(List.of(rule)))
                .build());

        log.debug("Applied {}-day lifecycle rule on bucket '{}' prefix '{}'",
                EXPIRY_DAYS, props.getBucket(), props.getPrefix());
    }
}

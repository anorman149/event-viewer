package org.eventviewer.s3;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.time.Duration;

public class S3ClientImpl implements S3Client {

    private static final Logger log = LoggerFactory.getLogger(S3ClientImpl.class);

    private final software.amazon.awssdk.services.s3.S3Client awsS3;
    private final S3Properties props;
    private final Retry retry;

    private final Timer createTimer;
    private final Timer getTimer;
    private final Timer deleteTimer;
    private final DistributionSummary bytesWrittenSummary;
    private final DistributionSummary bytesReadSummary;
    private final Counter putFailureCounter;
    private final Counter getFailureCounter;
    private final Counter totalOpsCounter;

    public S3ClientImpl(software.amazon.awssdk.services.s3.S3Client awsS3,
                        S3Properties props,
                        MeterRegistry registry) {
        this.awsS3 = awsS3;
        this.props = props;

        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(props.getMaxRetries())
                .waitDuration(Duration.ofMillis(500))
                .retryOnException(e ->
                        e instanceof SdkClientException ||
                        (e instanceof SdkServiceException sse && sse.statusCode() >= 500))
                .build();
        this.retry = Retry.of("s3", retryConfig);

        this.createTimer = Timer.builder("s3.operation.duration")
                .tag("operation", "create")
                .description("Time to complete an S3 PUT operation")
                .publishPercentileHistogram()
                .register(registry);
        this.getTimer = Timer.builder("s3.operation.duration")
                .tag("operation", "get")
                .description("Time to complete an S3 GET operation")
                .publishPercentileHistogram()
                .register(registry);
        this.deleteTimer = Timer.builder("s3.operation.duration")
                .tag("operation", "delete")
                .description("Time to complete an S3 DELETE operation")
                .publishPercentileHistogram()
                .register(registry);
        this.bytesWrittenSummary = DistributionSummary.builder("s3.bytes.written")
                .description("Bytes written per S3 PUT")
                .publishPercentileHistogram()
                .register(registry);
        this.bytesReadSummary = DistributionSummary.builder("s3.bytes.read")
                .description("Bytes read per S3 GET")
                .publishPercentileHistogram()
                .register(registry);
        this.putFailureCounter = Counter.builder("s3.operation.failures")
                .tag("operation", "create")
                .description("Total S3 PUT failures")
                .register(registry);
        this.getFailureCounter = Counter.builder("s3.operation.failures")
                .tag("operation", "get")
                .description("Total S3 GET failures")
                .register(registry);
        this.totalOpsCounter = Counter.builder("s3.operations.total")
                .description("Total S3 operations executed")
                .register(registry);
    }

    @Override
    public CreateKeyStep create() {
        return new CreateOperation();
    }

    @Override
    public GetKeyStep get() {
        return new GetOperation();
    }

    @Override
    public DeleteKeyStep delete() {
        return new DeleteOperation();
    }

    // ── Create ───────────────────────────────────────────────────────────────

    private class CreateOperation implements CreateKeyStep, CreateBodyStep, CreateExecuteStep {
        private String key;
        private byte[] body;

        @Override
        public CreateBodyStep key(String key) {
            this.key = key;
            return this;
        }

        @Override
        public CreateExecuteStep body(byte[] body) {
            this.body = body;
            return this;
        }

        @Override
        public CreateResult execute() {
            Timer.Sample sample = Timer.start();
            try {
                Retry.decorateRunnable(retry, () -> {
                    PutObjectRequest request = PutObjectRequest.builder()
                            .bucket(props.getBucket())
                            .key(key)
                            .contentLength((long) body.length)
                            .build();
                    awsS3.putObject(request, RequestBody.fromBytes(body));
                }).run();

                bytesWrittenSummary.record(body.length);
                totalOpsCounter.increment();
                log.debug("S3 PUT {} bytes to {}", body.length, key);
                return new CreateResult(key, body.length);
            } catch (Exception e) {
                putFailureCounter.increment();
                throw new S3OperationException("S3 PUT failed for key: " + key, e);
            } finally {
                sample.stop(createTimer);
            }
        }
    }

    // ── Get ──────────────────────────────────────────────────────────────────

    private class GetOperation implements GetKeyStep, GetExecuteStep {
        private String key;
        private Long rangeOffset;
        private Integer rangeLength;

        @Override
        public GetExecuteStep key(String key) {
            this.key = key;
            return this;
        }

        @Override
        public GetExecuteStep range(long offset, int length) {
            this.rangeOffset = offset;
            this.rangeLength = length;
            return this;
        }

        @Override
        public byte[] execute() {
            Timer.Sample sample = Timer.start();
            try {
                byte[] result = Retry.decorateCallable(retry, () -> {
                    GetObjectRequest.Builder builder = GetObjectRequest.builder()
                            .bucket(props.getBucket())
                            .key(key);
                    if (rangeOffset != null) {
                        long rangeEnd = rangeOffset + rangeLength - 1;
                        builder.range("bytes=" + rangeOffset + "-" + rangeEnd);
                    }
                    try (ResponseInputStream<GetObjectResponse> response = awsS3.getObject(builder.build())) {
                        return response.readAllBytes();
                    } catch (IOException e) {
                        throw new S3OperationException("Failed reading S3 response body for key: " + key, e);
                    }
                }).call();

                bytesReadSummary.record(result.length);
                totalOpsCounter.increment();
                log.debug("S3 GET {} bytes from {}", result.length, key);
                return result;
            } catch (S3OperationException e) {
                getFailureCounter.increment();
                throw e;
            } catch (Exception e) {
                getFailureCounter.increment();
                throw new S3OperationException("S3 GET failed for key: " + key, e);
            } finally {
                sample.stop(getTimer);
            }
        }
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    private class DeleteOperation implements DeleteKeyStep, DeleteExecuteStep {
        private String key;

        @Override
        public DeleteExecuteStep key(String key) {
            this.key = key;
            return this;
        }

        @Override
        public void execute() {
            Timer.Sample sample = Timer.start();
            try {
                Retry.decorateRunnable(retry, () -> {
                    DeleteObjectRequest request = DeleteObjectRequest.builder()
                            .bucket(props.getBucket())
                            .key(key)
                            .build();
                    awsS3.deleteObject(request);
                }).run();

                totalOpsCounter.increment();
                log.debug("S3 DELETE {}", key);
            } catch (Exception e) {
                throw new S3OperationException("S3 DELETE failed for key: " + key, e);
            } finally {
                sample.stop(deleteTimer);
            }
        }
    }
}

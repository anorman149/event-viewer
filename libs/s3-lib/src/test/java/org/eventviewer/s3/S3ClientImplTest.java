package org.eventviewer.s3;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3ClientImplTest {

    @Mock
    private software.amazon.awssdk.services.s3.S3Client awsS3;

    private S3Properties props;
    private S3ClientImpl s3Client;
    private SimpleMeterRegistry registry;

    @BeforeEach
    void setUp() {
        props = new S3Properties();
        props.setBucket("test-bucket");
        props.setRegion("us-east-1");
        props.setPrefix("events");
        props.setMaxRetries(1);

        registry = new SimpleMeterRegistry();
        s3Client = new S3ClientImpl(awsS3, props, registry);
    }

    @Test
    void create_putsObjectAndReturnsResult() {
        byte[] body = "test-content".getBytes(StandardCharsets.UTF_8);
        when(awsS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        CreateResult result = s3Client.create()
                .key("events/year=2026/test.zst")
                .body(body)
                .execute();

        assertThat(result.key()).isEqualTo("events/year=2026/test.zst");
        assertThat(result.bytesWritten()).isEqualTo(body.length);
        verify(awsS3).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void create_recordsBytesWrittenMetric() {
        byte[] body = new byte[1024];
        when(awsS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        s3Client.create().key("test-key").body(body).execute();

        assertThat(registry.find("s3.bytes.written").summary().totalAmount()).isEqualTo(1024.0);
    }

    @Test
    void get_withRange_fetchesByteRange() throws Exception {
        byte[] responseBytes = "compressed-blob".getBytes(StandardCharsets.UTF_8);
        ResponseInputStream<GetObjectResponse> stream = new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                AbortableInputStream.create(new ByteArrayInputStream(responseBytes)));
        when(awsS3.getObject(any(GetObjectRequest.class))).thenReturn(stream);

        byte[] result = s3Client.get()
                .key("events/test.zst")
                .range(100, 15)
                .execute();

        assertThat(result).isEqualTo(responseBytes);
        verify(awsS3).getObject(any(GetObjectRequest.class));
    }

    @Test
    void get_withoutRange_fetchesFullObject() throws Exception {
        byte[] responseBytes = "full-content".getBytes(StandardCharsets.UTF_8);
        ResponseInputStream<GetObjectResponse> stream = new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                AbortableInputStream.create(new ByteArrayInputStream(responseBytes)));
        when(awsS3.getObject(any(GetObjectRequest.class))).thenReturn(stream);

        byte[] result = s3Client.get()
                .key("events/test.zst")
                .execute();

        assertThat(result).isEqualTo(responseBytes);
    }

    @Test
    void delete_deletesObject() {
        s3Client.delete().key("events/old.zst").execute();

        verify(awsS3).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void create_onFailure_throwsS3OperationException() {
        when(awsS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(new RuntimeException("network error"));

        assertThatThrownBy(() -> s3Client.create()
                .key("events/test.zst")
                .body(new byte[10])
                .execute())
                .isInstanceOf(S3OperationException.class);
    }

    @Test
    void create_onFailure_incrementsFailureCounter() {
        when(awsS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(new RuntimeException("network error"));

        try {
            s3Client.create().key("k").body(new byte[1]).execute();
        } catch (S3OperationException ignored) {}

        assertThat(registry.find("s3.operation.failures")
                .tag("operation", "create").counter().count()).isGreaterThan(0);
    }
}

package org.eventviewer.opensearch.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.util.TimeValue;
import org.eventviewer.opensearch.OsAdminClient;
import org.eventviewer.opensearch.OsDocumentClient;
import org.eventviewer.opensearch.OsMigration;
import org.eventviewer.opensearch.OsSchemaRegistry;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.util.List;

@AutoConfiguration
@EnableConfigurationProperties(OsProperties.class)
public class OsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public OsSchemaRegistry osSchemaRegistry() {
        return new OsSchemaRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    public OpenSearchClient openSearchClient(OsProperties props) {
        String scheme = props.isUseSsl() ? "https" : "http";
        HttpHost host = new HttpHost(scheme, props.getHost(), props.getPort());

        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper(objectMapper);

        DefaultHttpRequestRetryStrategy retryStrategy = new DefaultHttpRequestRetryStrategy(3, TimeValue.ofSeconds(1)) {
            @Override
            protected boolean handleAsIdempotent(HttpRequest request) {
                return true;
            }
        };

        var builder = ApacheHttpClient5TransportBuilder.builder(host)
                .setMapper(jsonpMapper)
                .setHttpClientConfigCallback(cb -> cb.setRetryStrategy(retryStrategy));

        if (props.getUsername() != null && !props.getUsername().isBlank()) {
            org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider credProvider =
                    new org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider();
            credProvider.setCredentials(
                    new org.apache.hc.client5.http.auth.AuthScope(props.getHost(), props.getPort()),
                    new org.apache.hc.client5.http.auth.UsernamePasswordCredentials(
                            props.getUsername(),
                            props.getPassword() != null ? props.getPassword().toCharArray() : new char[0]));
            builder.setHttpClientConfigCallback(cb ->
                    cb.setRetryStrategy(retryStrategy)
                      .setDefaultCredentialsProvider(credProvider));
        }

        return new OpenSearchClient(builder.build());
    }

    @Bean
    public OsClient osClient(OpenSearchClient openSearchClient,
                              OsSchemaRegistry registry,
                              MeterRegistry meterRegistry) {
        return new OsClient(openSearchClient, registry, meterRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public OsAdminClient osAdminClient(OsClient osClient) {
        return osClient;
    }

    @Bean
    @Primary
    @ConditionalOnMissingBean
    public OsDocumentClient osDocumentClient(OsClient osClient) {
        return osClient;
    }

    @Bean
    public OsSchemaManager osSchemaManager(OsAdminClient adminClient,
                                            OpenSearchClient openSearchClient,
                                            OsProperties properties,
                                            List<OsMigration> migrations,
                                            MeterRegistry meterRegistry) {
        return new OsSchemaManager(adminClient, openSearchClient, properties, migrations, meterRegistry);
    }
}

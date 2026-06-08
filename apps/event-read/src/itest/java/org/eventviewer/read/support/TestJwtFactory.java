package org.eventviewer.read.support;

import io.jsonwebtoken.Jwts;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Date;

public final class TestJwtFactory {

    private static final PrivateKey PRIVATE_KEY = loadPrivateKey();

    private TestJwtFactory() {}

    public static String generateJwt(String subject) {
        long now = System.currentTimeMillis();
        return Jwts.builder()
                .subject(subject)
                .issuer("event-viewer-dev")
                .issuedAt(new Date(now))
                .expiration(new Date(now + 3_600_000))
                .signWith(PRIVATE_KEY, Jwts.SIG.RS256)
                .compact();
    }

    private static PrivateKey loadPrivateKey() {
        try (InputStream is = TestJwtFactory.class.getResourceAsStream("/keys/local-private.pem")) {
            if (is == null) {
                throw new IllegalStateException("local-private.pem not found at /keys/local-private.pem");
            }
            String pem = new String(is.readAllBytes(), StandardCharsets.UTF_8)
                    .replace("-----BEGIN PRIVATE KEY-----", "")
                    .replace("-----END PRIVATE KEY-----", "")
                    .replaceAll("\\s", "");
            byte[] keyBytes = Base64.getDecoder().decode(pem);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return kf.generatePrivate(new PKCS8EncodedKeySpec(keyBytes));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load local private key", e);
        }
    }
}

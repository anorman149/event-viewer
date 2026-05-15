package org.eventviewer.s3;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ZstdCodec {

    private static final int LEVEL = 3;

    public byte[] compress(byte[] data) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
            try (ZstdOutputStream zos = new ZstdOutputStream(baos, LEVEL)) {
                zos.write(data);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("ZSTD compression failed", e);
        }
    }

    public byte[] decompress(byte[] compressed) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ZstdInputStream zis = new ZstdInputStream(new ByteArrayInputStream(compressed))) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = zis.read(buf)) != -1) {
                    baos.write(buf, 0, n);
                }
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("ZSTD decompression failed", e);
        }
    }
}

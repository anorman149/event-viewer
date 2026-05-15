package org.eventviewer.s3;

public interface S3Client {

    CreateKeyStep create();

    GetKeyStep get();

    DeleteKeyStep delete();

    // ── Create step builder ──────────────────────────────────────────────────

    interface CreateKeyStep {
        CreateBodyStep key(String key);
    }

    interface CreateBodyStep {
        CreateExecuteStep body(byte[] body);
    }

    interface CreateExecuteStep {
        CreateResult execute();
    }

    // ── Get step builder ─────────────────────────────────────────────────────

    interface GetKeyStep {
        GetExecuteStep key(String key);
    }

    interface GetExecuteStep {
        /** Byte-range fetch: offset is inclusive start, length is number of bytes. */
        GetExecuteStep range(long offset, int length);

        byte[] execute();
    }

    // ── Delete step builder ──────────────────────────────────────────────────

    interface DeleteKeyStep {
        DeleteExecuteStep key(String key);
    }

    interface DeleteExecuteStep {
        void execute();
    }
}

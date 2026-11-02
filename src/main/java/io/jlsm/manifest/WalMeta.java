package io.jlsm.manifest;

public record WalMeta(long walId, boolean isSealed) {
    public WalMeta {
        if (walId < 0) {
            throw new IllegalArgumentException("walId cannot be negative: " + walId);
        }
    }
}

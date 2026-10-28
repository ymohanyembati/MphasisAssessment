package com.fde.txnprocessor.resilience;

/** Bounded retries with exponential backoff. The sleeper is injectable so tests run instantly. */
public final class RetryPolicy {
    @FunctionalInterface
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final int maxAttempts;
    private final long baseDelayMillis;
    private final Sleeper sleeper;

    public RetryPolicy(int maxAttempts, long baseDelayMillis, Sleeper sleeper) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.maxAttempts = maxAttempts;
        this.baseDelayMillis = baseDelayMillis;
        this.sleeper = sleeper;
    }

    public static RetryPolicy noDelay(int maxAttempts) {
        return new RetryPolicy(maxAttempts, 0, millis -> { });
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    /** Waits before the next attempt, after the given (1-based) failed attempt. */
    public void pauseAfter(int failedAttempt) {
        long delay = Math.min(baseDelayMillis * (1L << Math.min(failedAttempt - 1, 10)), 5_000L);
        if (delay <= 0) {
            return;
        }
        try {
            sleeper.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

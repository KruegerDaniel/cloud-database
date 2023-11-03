package de.tum.i13.shared;

import java.util.Random;

public class BackOff {
    private final int max;
    private final int base;
    private int attempt = 0;

    public BackOff(int base, int max) {
        this.max = max;
        this.base = base;
    }

    /**
     * Sleeps for a radom amount of time (0...min(cap, base * 2^attempt)). Implementation of exponential backoff with
     * full jitter.
     */
    public void backOffWithJitter() {
        attempt++;
        try {
            int time = new Random().nextInt((int) Math.min(max, base * Math.pow(2, attempt)));
            Thread.sleep(time);
        } catch (InterruptedException ignored) {

        }
    }

    public void resetBackOff() {
        this.attempt = 0;
    }
}

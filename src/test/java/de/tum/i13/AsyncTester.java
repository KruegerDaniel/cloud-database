package de.tum.i13;

/**
 * This class lets you assert using threads in testing. Create an AsyncTester like a Thread while passing a
 * Runnable and then call start(). To make the AsyncTester fail the test in the main method, throw new AssertionError().
 * At the end of the test, call test().
 */
public class AsyncTester {
    private final Thread thread;
    private volatile AssertionError error;

    public AsyncTester(Runnable runnable) {
        this.thread = new Thread(() -> {
            try {
                runnable.run();
            } catch (AssertionError e) {
                error = e;
            }
        });
    }

    public void start() {
        thread.start();
    }

    public void test() throws InterruptedException {
        thread.join();
        if (error != null) {
            throw error;
        }
    }
}

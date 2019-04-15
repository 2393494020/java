package org.who.stream;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Sleeper {
    private static final Random RANDOM = new Random();

    static void randSleep(double mean, double stdDev) {
        final double micros = 1_000 * (mean + RANDOM.nextGaussian() * stdDev);
        try {
            TimeUnit.MICROSECONDS.sleep((long) micros);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

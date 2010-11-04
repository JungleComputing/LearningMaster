package ibis.learningmaster;

import java.util.Random;

class ZeroClampedGaussianSource {
    private final Random rng = new Random();
    private final double mean;
    private final double stdDev;

    ZeroClampedGaussianSource(double mean, double stdDev) {
        this.mean = mean;
        this.stdDev = stdDev;
    }

    double next() {
        return Math.max(0, mean + rng.nextGaussian() * stdDev);
    }
}

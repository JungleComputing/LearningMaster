package ibis.learningmaster;

import java.util.Random;

class GaussianSource {
    private final Random rng = new Random();
    private final double mean;
    private final double stdDev;

    GaussianSource(double mean, double stdDev) {
        this.mean = mean;
        this.stdDev = stdDev;
    }

    double next() {
        return mean + rng.nextGaussian() * stdDev;
    }
}

package ibis.learningmaster;

import java.util.Arrays;

class GaussianEstimator implements EstimatorInterface {
    private double values[] = new double[10];
    private double sum = 0.0;
    private int sampleCount = 0;

    @Override
    public void addSample(final double value) {
        if (sampleCount >= values.length) {
            final double nw[] = Arrays.copyOf(values, values.length * 2);
            values = nw;
        }
        values[sampleCount++] = value;
        sum += value;
    }

    @Override
    public double getAverage() {
        return sum / sampleCount;
    }

    double getStdDev() {
        if (sampleCount < 2) {
            return Double.POSITIVE_INFINITY;
        }
        final double sumDev = computeErrorSum();
        return Math.sqrt(sumDev / (sampleCount - 1));
    }

    private double computeErrorSum() {
        final double av = sum / sampleCount;
        double sumDev = 0.0;
        for (int i = 0; i < sampleCount; i++) {
            final double v = values[i];
            sumDev += Math.abs(v - av);
        }
        return sumDev;
    }

    // FIXME: this is just an intuitive approximation of a likely values comp.
    double getLikelyError() {
        final double av = sum / sampleCount;
        return getStdDev() + av / Math.sqrt(sampleCount);
    }

    private static double getLikelyValue(final double average,
            final double stdDev) {
        return average + stdDev * Globals.rng.nextGaussian();
    }

    private static void calculate(final double l[]) {
        final GaussianEstimator est = new GaussianEstimator();
        for (final double v : l) {
            est.addSample(v);
        }
        final double average = est.getAverage();
        final double err = est.getLikelyError();
        est.printStatistics(null);

        System.out.print("  likely values:");
        for (int i = 0; i < 15; i++) {
            System.out.format(" %.3g", getLikelyValue(average, err));
        }
        System.out.println();
    }

    public static void main(final String args[]) {
        calculate(new double[] { 42.0 });
        calculate(new double[] { 0, 0, 12, 12 });
        calculate(new double[] { 13.0, 17.1, 15.6, 22.1, 14.1, 11.2, 14.1,
                12.4, 29.3 });
    }

    @Override
    public double getLikelyValue() {
        final double average = getAverage();
        final double err = getLikelyError();
        return getLikelyValue(average, err);
    }

    @Override
    public void printStatistics(final String lbl) {
        final double average = getAverage();
        final double stdDev = getStdDev();
        final double err = getLikelyError();
        if (lbl != null) {
            System.out.print(lbl);
            System.out.print(": ");
        }
        System.out.println("samples=" + sampleCount + " average=" + average
                + " stdDev=" + stdDev + " likely error=" + err);
    }

    @Override
    public void setInitialEstimate(double v) {
        addSample(v);
    }
}

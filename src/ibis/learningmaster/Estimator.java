package ibis.learningmaster;

import java.util.Arrays;

class Estimator {
    private double values[] = new double[10];
    private double sum = 0.0;
    private int sampleCount = 0;

    void addSample(double value) {
        if (sampleCount >= values.length) {
            final double nw[] = Arrays.copyOf(values, values.length * 2);
            values = nw;
        }
        values[sampleCount++] = value;
        sum += value;
    }

    double getAverage() {
        return sum / sampleCount;
    }

    double getStdDev() {
        if (sampleCount == 0) {
            return Double.POSITIVE_INFINITY;
        }
        final double sumDev = computeErrorSum();
        return Math.sqrt(sumDev / sampleCount);
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
        // return getStdDev() + Math.sqrt((10 * av) / sampleCount);
        return getStdDev() + (av / Math.sqrt(sampleCount));
    }

    private static double getLikelyValue(double average, double stdDev) {
        return average + stdDev * Globals.rng.nextGaussian();
    }

    private static void calculate(double l[]) {
        final Estimator est = new Estimator();
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

    public static void main(String args[]) {
        calculate(new double[] { 42.0 });
        calculate(new double[] { 0, 0, 12, 12 });
        calculate(new double[] { 13.0, 17.1, 15.6, 22.1, 14.1, 11.2, 14.1,
                12.4, 29.3 });
    }

    double getLikelyValue() {
        final double average = getAverage();
        final double err = getLikelyError();
        return getLikelyValue(average, err);
    }

    void printStatistics(String lbl) {
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
}

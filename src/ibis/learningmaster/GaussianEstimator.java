package ibis.learningmaster;

import java.io.PrintStream;

class GaussianEstimator implements EstimatorInterface {
    private double average = 0.0;
    private double S = 0.0;
    private int sampleCount = 0;

    @Override
    public void addSample(final double value) {
        sampleCount++;
        final double oldAverage = average;
        average += (value - average) / sampleCount;
        S += (value - oldAverage) * (value - average);
    }

    @Override
    public double getAverage() {
        return average;
    }

    @Override
    public double getStdDev() {
        if (sampleCount < 2) {
            return Double.POSITIVE_INFINITY;
        }
        return Math.sqrt(S / (sampleCount - 1));
    }

    // FIXME: this is just an intuitive approximation of a likely values comp.
    double getLikelyError() {
        final double stdDev = getStdDev();
        return stdDev + 0.1 * average / Math.sqrt(sampleCount);
    }

    private static double getLikelyValue(final double average,
            final double stdDev) {
        return average + stdDev * Globals.rng.nextGaussian();
    }

    @Override
    public double getPessimisticEstimate() {
        if (sampleCount < 2) {
            return Double.POSITIVE_INFINITY;
        }
        return average + Math.sqrt(S / (sampleCount - 1));
    }

    private static void calculate(final double l[]) {
        final GaussianEstimator est = new GaussianEstimator();
        for (final double v : l) {
            est.addSample(v);
        }
        final double average = est.getAverage();
        final double err = est.getLikelyError();
        est.printStatistics(System.out, null);

        System.out.print("  likely values:");
        for (int i = 0; i < 15; i++) {
            System.out.format(" %.3g", getLikelyValue(average, err));
        }
        System.out.println();
    }

    public static void main(final String args[]) {
        calculate(new double[] { 42.0 });
        calculate(new double[] { 4, 9, 11, 12, 17, 5, 8, 12, 14 });
        calculate(new double[] { 0, 0, 12, 12, 6 });
        calculate(new double[] { 13.0, 17.1, 15.6, 22.1, 14.1, 11.2, 14.1,
                12.4, 29.3 });
        calculate(new double[] { 13.0, 17.1, 15.6, 22.1, 14.1, 11.2, 14.1,
                12.4, 29.3, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5, 16.5,
                16.5, 16.5, 16.5, 16.5, });
    }

    @Override
    public double getLikelyValue() {
        final double err = getLikelyError();
        return getLikelyValue(average, err);
    }

    @Override
    public void printStatistics(final PrintStream s, final String lbl) {
        final double stdDev = getStdDev();
        final double err = getLikelyError();
        if (lbl != null) {
            s.print(lbl);
            s.print(": ");
        }
        s.println("samples=" + sampleCount + " average=" + average + " stdDev="
                + stdDev + " likely error=" + err);
    }

    @Override
    public void setInitialEstimate(final double v) {
        addSample(v);
    }

    @Override
    public String getName() {
        return "gaussian";
    }

    @Override
    public int getSampleCount() {
        return sampleCount;
    }
}

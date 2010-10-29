package ibis.learningmaster;

import java.io.PrintStream;

class ExponentialDecayEstimator implements EstimatorInterface {
	private double average = 0.0;
	private double errorSquared = 0.0;
	private final double alpha = 0.33;
	private int sampleCount = 0;

	@Override
	public void addSample(final double value) {
		average = alpha * value + (1 - alpha) * average;
		final double delta = value - average;
		errorSquared += delta * delta;
		sampleCount++;
	}

	@Override
	public double getAverage() {
		return average;
	}

	double getStdDev() {
		return Math.sqrt(alpha * errorSquared);
	}

	// FIXME: this is just an intuitive approximation of a likely values comp.
	double getLikelyError() {
		return getStdDev() + average / Math.sqrt(sampleCount);
	}

	private static double getLikelyValue(final double average,
			final double stdDev) {
		return average + stdDev * Globals.rng.nextGaussian();
	}

	private static void calculate(final double l[]) {
		final ExponentialDecayEstimator est = new ExponentialDecayEstimator();
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
		calculate(new double[] { 0, 0, 12, 12 });
		calculate(new double[] { 13.0, 17.1, 15.6, 22.1, 14.1, 11.2, 14.1,
				12.4, 29.3 });
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
		average = v;
	}
}

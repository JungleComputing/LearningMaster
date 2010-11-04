package ibis.learningmaster;

import java.io.PrintStream;

class ExponentialDecayEstimator implements EstimatorInterface {
	private double average = 0.0;
	private double variance = 0.0;
	private final double alpha;
	private int sampleCount = 0;

	public ExponentialDecayEstimator() {
		this(0.25);
	}

	public ExponentialDecayEstimator(final double alpha) {
		this.alpha = alpha;
	}

	@Override
	public void addSample(final double x) {
		final double diff = x - average;
		final double incr = alpha * diff;
		average += incr;
		variance = (1 - alpha) * variance + diff * incr;
		sampleCount++;
	}

	@Override
	public double getAverage() {
		return average;
	}

	@Override
	public double getPessimisticEstimate() {
		if (sampleCount < 2) {
			return Double.POSITIVE_INFINITY;
		}
		return average + Math.sqrt(variance);
	}

	public double getStdDev() {
		return Math.sqrt(variance);
	}

	// FIXME: this is just an intuitive approximation of a likely values comp.
	private double getLikelyError() {
		// return getStdDev() + average / Math.sqrt(sampleCount);
		return getStdDev() / (1 - alpha);
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
		average = v;
	}

	@Override
	public String getName() {
		return "exponential-decay";
	}
}

package ibis.learningmaster;

import ibis.steel.EstimatorInterface;
import ibis.steel.ExponentialDecayEstimator;

class SingleLearner {
	private static final int SAMPLES = 300;

	private static void runLearningExperiment() {
		final ZeroClampedGaussianSource src = new ZeroClampedGaussianSource(
				100, 5);
		// final EstimatorInterface estimator = new GaussianEstimator();
		final EstimatorInterface estimator = new ExponentialDecayEstimator(0.1);

		for (int i = 1; i <= SAMPLES; i++) {
			final double v = src.next();
			estimator.addSample(v);
			System.out.println(i + ": " + v + " " + estimator.getAverage()
					+ " " + estimator.getStdDev() + " "
					+ estimator.getLikelyValue());
		}
	}

	public static void main(final String args[]) {
		runLearningExperiment();
	}
}

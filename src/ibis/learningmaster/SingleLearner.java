package ibis.learningmaster;

import ibis.steel.Estimator;
import ibis.steel.GaussianDecayingEstimator;

class SingleLearner {
    private static final int SAMPLES = 300;

    private static void runLearningExperiment() {
        final ZeroClampedGaussianSource src = new ZeroClampedGaussianSource(
                100, 5);
        // final EstimatorInterface estimator = new GaussianEstimator();
        final Estimator estimator = new GaussianDecayingEstimator(0, 0, 0.1);

        for (int i = 1; i <= SAMPLES; i++) {
            final double v = src.next();
            estimator.addSample(v);
            System.out.println(i + ": " + v + " "
                    + estimator.getStatisticsString() + " "
                    + estimator.getLikelyValue());
        }
    }

    public static void main(final String args[]) {
        runLearningExperiment();
    }
}

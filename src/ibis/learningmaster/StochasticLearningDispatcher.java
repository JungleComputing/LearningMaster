package ibis.learningmaster;

import ibis.steel.Estimator;
import ibis.steel.GaussianDecayingEstimator;

import java.io.PrintStream;

class StochasticLearningDispatcher {
    private static final int GENERATORS = 20;
    private static int JOBCOUNT = 30000;
    private static final double STDDEV = 0.20;

    private static class Worker {
        final ZeroClampedGaussianSource src;

        Worker(final double average, final double likelyError) {
            src = new ZeroClampedGaussianSource(average, likelyError);
        }

        double getValue() {
            return src.next();
        }
    }

    private static class WorkerEstimator {
        private final Estimator performance = buildEstimator();

        static Estimator buildEstimator() {
            return new GaussianDecayingEstimator(0, 0);
            // return new GaussianEstimator();
        }

        WorkerEstimator() {
            // Start with a very optimistic estimate to avoid corner cases
            performance.addSample(0);
        }

        public double getLikelyValue() {
            return Math.max(0, performance.getLikelyValue());
        }

        public void addSample(final double v) {
            performance.addSample(v);
        }

        public void printStatistics(final PrintStream s, final String lbl) {
            s.println(lbl + ": " + performance.getName() + ": "
                    + performance.getStatisticsString());
        }
    }

    private static final Worker workers[] = new Worker[GENERATORS];
    private static final WorkerEstimator workerEstimators[] = new WorkerEstimator[GENERATORS];

    private static double submitAJob(final boolean verbose, final Worker[] wl,
            final WorkerEstimator[] wel) {
        double bestOffer = Double.MAX_VALUE;
        int bestWorker = -1;
        for (int i = 0; i < wl.length; i++) {
            final WorkerEstimator w = wel[i];
            final double offer = w.getLikelyValue();
            if (bestOffer > offer) {
                bestOffer = offer;
                bestWorker = i;
            }
        }
        final double resultValue = wl[bestWorker].getValue();
        if (verbose) {
            System.out.println("Worker " + bestWorker + " -> " + resultValue);
        }
        wel[bestWorker].addSample(resultValue);
        return resultValue;
    }

    private static void runExperiment(final PrintStream stream,
            final String lbl, final boolean verbose,
            final boolean printEndStats, final double fast,
            final double normal, final double slow, final double stddev,
            final int jobCount) {
        for (int i = 0; i < GENERATORS; i++) {
            double v = normal;
            if (i == 1) {
                v = fast;
            } else if (i == 2) {
                v = slow;
            }
            workers[i] = new Worker(v, stddev * v);
            workerEstimators[i] = new WorkerEstimator();
        }
        double cost = 0;
        for (int i = 0; i < jobCount; i++) {
            cost += submitAJob(verbose, workers, workerEstimators);
        }
        if (printEndStats) {
            for (int i = 0; i < workerEstimators.length; i++) {
                workerEstimators[i].printStatistics(System.out,
                        Integer.toString(i));
            }
            System.out.format(
                    "Fast: %3g normal: %3g slow: %3g  Average cost: %.3g\n",
                    fast, normal, slow, cost / jobCount);
        } else {
            stream.println(lbl + " " + cost / jobCount);
        }
    }

    public static void main(final String args[]) {
        runExperiment(System.out, "test", false, true, 100, 500, 800, STDDEV,
                JOBCOUNT);
    }
}

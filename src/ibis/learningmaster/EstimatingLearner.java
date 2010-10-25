package ibis.learningmaster;

class EstimatingLearner {
    private static final int WORKERS = 20;

    private static class Worker {
        private final double average;
        private final double likelyError;

        Worker(double average, double likelyError) {
            super();
            this.average = average;
            this.likelyError = likelyError;
        }

        double getValue() {
            return average + likelyError * Globals.rng.nextGaussian();
        }
    }

    private static class WorkerEstimator {
        private final Estimator performance = new Estimator();

        WorkerEstimator() {
            // Start with a very optimistic estimate to avoid corner cases
            performance.addSample(0);
        }

        public double getLikelyValue() {
            return performance.getLikelyValue();
        }

        public void addSample(double v) {
            performance.addSample(v);
        }

        public void printStatistics(String lbl) {
            performance.printStatistics(lbl);
        }
    }

    private static final Worker workers[] = new Worker[WORKERS];
    private static final WorkerEstimator workerEstimators[] = new WorkerEstimator[WORKERS];

    private static double submitAJob(boolean verbose, Worker[] wl,
            WorkerEstimator[] wel) {
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

    private static void runExperiment(boolean verbose, double fast,
            double normal, double slow, double stddev, int jobCount) {
        for (int i = 0; i < WORKERS; i++) {
            double v = normal;
            if ((i % 4) == 1) {
                v = fast;
            } else if ((i % 4) == 0) {
                v = slow;
            }
            workers[i] = new Worker(v, stddev * v);
            workerEstimators[i] = new WorkerEstimator();
        }
        double cost = 0;
        for (int i = 0; i < jobCount; i++) {
            cost += submitAJob(verbose, workers, workerEstimators);
        }
        if (verbose) {
            for (int i = 0; i < workerEstimators.length; i++) {
                workerEstimators[i].printStatistics(Integer.toString(i));
            }
            System.out.format("Average cost: %.3g\n", cost / jobCount);
        } else {
            System.out.println((fast / normal) + " " + (cost / jobCount));
        }
    }

    // - sweep advantage of best worker from 0.1 to 0.9
    private static void generatePoints(double slowFactor) {
        final double advantages[] = { 0.01, 0.1, 0.25, 0.5, 0.7, 0.9, 0.95 };
        final int SAMPLES = 20;
        final double fast = 100;

        for (final double a : advantages) {
            final double normal = fast / a;
            final double slow = slowFactor * normal;

            for (int sample = 0; sample < SAMPLES; sample++) {
                runExperiment(false, fast, normal, slow, 0.01, 30000);
            }
        }
    }

    public static void main(String args[]) {
        // runExperiment(true, 0.2, 100.0, 10000);
        generatePoints(10);
    }
}

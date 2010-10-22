package ibis.learningmaster;

class EstimatingLearner {
    private static final int WORKERS = 6;

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

        public double getLikelyValue() {
            return performance.getLikelyValue();
        }

        public void addSample(double v) {
            performance.addSample(v);
        }

        public void printStatistics() {
            performance.printStatistics();
        }
    }

    private static final Worker workers[] = new Worker[WORKERS];
    private static final WorkerEstimator workerEstimators[] = new WorkerEstimator[WORKERS];

    @SuppressWarnings("synthetic-access")
    private static void runExperiment(double av, int jobCount) {
        for (int i = 0; i < WORKERS; i++) {
            double v = av;
            if (i == 1) {
                v = 0.9 * av;
            }
            workers[i] = new Worker(v, 0.01 * av);
            workerEstimators[i] = new WorkerEstimator();
            workerEstimators[i].addSample(av);
        }

        for (int i = 0; i < jobCount; i++) {
            submitAJob(workers, workerEstimators);
        }
        for (int i = 0; i < workerEstimators.length; i++) {
            workerEstimators[i].printStatistics();
        }
    }

    private static void submitAJob(Worker[] wl, WorkerEstimator[] wel) {
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
        System.out.println("Worker " + bestWorker + " -> " + resultValue);
        wel[bestWorker].addSample(resultValue);
    }

    public static void main(String args[]) {
        runExperiment(20.0, 1000);
    }
}

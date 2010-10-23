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

    private static double submitAJob(Worker[] wl, WorkerEstimator[] wel) {
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
        return resultValue;
    }

    @SuppressWarnings("synthetic-access")
    private static void runExperiment(double av, int jobCount) {
        for (int i = 0; i < WORKERS; i++) {
            double v = av;
            if ((i % 4) == 1) {
                v = 0.2 * av;
            } else if ((i % 4) == 0) {
                v = 20 * av;
            }
            workers[i] = new Worker(v, 0.01 * v);
            workerEstimators[i] = new WorkerEstimator();
            workerEstimators[i].addSample(0);
        }
        double cost = 0;
        for (int i = 0; i < jobCount; i++) {
            cost += submitAJob(workers, workerEstimators);
        }
        for (int i = 0; i < workerEstimators.length; i++) {
            workerEstimators[i].printStatistics(Integer.toString(i));
        }
        System.out.format("Average cost: %.3g\n", cost / jobCount);
    }

    public static void main(String args[]) {
        runExperiment(100.0, 10000);
    }
}

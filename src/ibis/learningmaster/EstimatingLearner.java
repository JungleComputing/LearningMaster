package ibis.learningmaster;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

class EstimatingLearner {
    private static final int WORKERS = 20;

    private static class Worker {
        private final double average;
        private final double likelyError;

        Worker(final double average, final double likelyError) {
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

        public void addSample(final double v) {
            performance.addSample(v);
        }

        public void printStatistics(final String lbl) {
            performance.printStatistics(lbl);
        }
    }

    private static final Worker workers[] = new Worker[WORKERS];
    private static final WorkerEstimator workerEstimators[] = new WorkerEstimator[WORKERS];
    private static final int SAMPLES = 10;
    private static int JOBCOUNT = 30000;
    private static final double STDDEV = 0.1;

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

    private static void runExperiment(final PrintStream stream, String lbl,
            final boolean verbose, final boolean printEndStats,
            final double fast, final double normal, final double slow,
            final double stddev, final int jobCount) {
        for (int i = 0; i < WORKERS; i++) {
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
                workerEstimators[i].printStatistics(Integer.toString(i));
            }
            System.out.format(
                    "Fast: %3g normal: %3g slow: %3g  Average cost: %.3g\n",
                    fast, normal, slow, cost / jobCount);
        } else {
            stream.println(lbl + " " + cost / jobCount);
        }
    }

    private static PrintStream openPrintFile(final String s) {
        try {
            return new PrintStream(new File(s));
        } catch (final FileNotFoundException e) {
            System.err.println("Cannot open file '" + s + "' for writing: "
                    + e.getLocalizedMessage());
            e.printStackTrace();
            System.exit(1);
            return null; // To satisfy the compiler.
        }
    }

    private static void runNormalSlowdownExperiments() {
        final double slowdowns[] = { 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50,
                100, 200 };
        final double fast = 100;
        final double slowFactor = 1;
        PrintStream stream;
        final String fnm = "normal-slowdown.data";
        stream = openPrintFile(fnm);

        for (final double s : slowdowns) {
            final double normal = fast * (1 + s);
            final double slow = slowFactor * normal;

            for (int sample = 0; sample < SAMPLES; sample++) {
                final String lbl = Double.toString(s);
                runExperiment(stream, lbl, false, false, fast, normal, slow,
                        STDDEV, JOBCOUNT);
            }
        }
        stream.close();
        System.out.println("Wrote file '" + fnm + "'");
    }

    private static void runStdDevExperiments() {
        final double stddevs[] = { 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10 };
        final double fast = 100;
        final double normal = 3 * fast;
        final double slow = normal;
        PrintStream stream;
        final String fnm = "stddev.data";
        stream = openPrintFile(fnm);

        for (final double s : stddevs) {

            for (int sample = 0; sample < SAMPLES; sample++) {
                final String lbl = Double.toString(s);
                runExperiment(stream, lbl, false, false, fast, normal, slow, s,
                        JOBCOUNT);
            }
        }
        stream.close();
        System.out.println("Wrote file '" + fnm + "'");
    }

    private static void runSampleCountExperiments() {
        final int samples[] = { 100, 200, 500, 1000, 2000, 5000, 10000, 20000,
                50000, 100000 };
        final double fast = 100;
        final double normal = 10 * fast;
        final double slow = normal;
        PrintStream stream;
        final String fnm = "samplecount.data";
        stream = openPrintFile(fnm);

        for (final int jobCount : samples) {

            for (int sample = 0; sample < SAMPLES; sample++) {
                final String lbl = Integer.toString(jobCount);
                runExperiment(stream, lbl, false, false, fast, normal, slow,
                        STDDEV, jobCount);
            }
        }
        stream.close();
        System.out.println("Wrote file '" + fnm + "'");
    }

    public static void main(final String args[]) {
        runNormalSlowdownExperiments();
        runStdDevExperiments();
        runSampleCountExperiments();
    }
}

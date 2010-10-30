package ibis.learningmaster;

import ibis.learningmaster.EventQueue.Event;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

class StochasticLearningMaster {
    private static final int WORKERS = 20;
    private static final int SAMPLES = 10;
    private static int JOBCOUNT = 30000;
    private static final double STDDEV = 0.20;
    private final EventQueue q = new EventQueue();
    private double now = 0;
    private final boolean verbose;

    public StochasticLearningMaster(boolean verbose) {
        this.verbose = verbose;
    }

    private static class JobCompletedEvent extends EventQueue.Event {
        final WorkerEstimator worker;
        final double completionTime;

        public JobCompletedEvent(double t, double completionTime,
                WorkerEstimator worker) {
            super(t);
            this.completionTime = completionTime;
            this.worker = worker;
        }

        @Override
        public String toString() {
            return "JobCompletedEvent[when=" + super.time + ",ct="
                    + completionTime + ",worker=" + worker + "]";
        }
    }

    private static class JobArrivedEvent extends EventQueue.Event {
        JobArrivedEvent(double time) {
            super(time);
        }

        @Override
        public String toString() {
            return "JobArrivedEvent[when=" + super.time + "]";
        }
    }

    private class Worker {
        GaussianSource workTimeGenerator;
        private double busyUntilTime;

        Worker(final double average, final double stdDev) {
            super();
            workTimeGenerator = new GaussianSource(average, stdDev);
        }

        /**
         * Execute a job on this worker. We place an event on the event queue to
         * mark the completion of this job. If the worker is
         */
        void executeJob(WorkerEstimator est) {
            final double t = workTimeGenerator.next();
            final double ct = Math.max(now, busyUntilTime) + t;
            final JobCompletedEvent e = new JobCompletedEvent(ct, t, est);
            q.addEvent(e);
        }
    }

    private static class WorkerEstimator {
        private final EstimatorInterface performance = buildEstimator();
        private int queueLength = 0;

        static EstimatorInterface buildEstimator() {
            return new ExponentialDecayEstimator();
            // return new GaussianEstimator();
        }

        WorkerEstimator() {
            // Start with a very optimistic estimate to avoid corner cases
            performance.addSample(0);
        }

        public void printStatistics(final PrintStream s, final String lbl) {
            performance.printStatistics(s, performance.getName() + ": " + lbl);
        }

        static String getName() {
            return buildEstimator().getName();
        }

        double estimateCompletionTime() {
            double t = 0;
            // FIXME: account for time the first job
            // is already busy.
            for (int i = 0; i <= queueLength; i++) {
                t += performance.getLikelyValue();
            }
            return t;
        }

        void registerQueuedJob() {
            queueLength++;
        }

        void registerCompletedJob(double completionTime) {
            queueLength--;
            performance.addSample(completionTime);
        }
    }

    private static final Worker workers[] = new Worker[WORKERS];
    private static final WorkerEstimator workerEstimators[] = new WorkerEstimator[WORKERS];

    double JOBINTERVAL = 1;
    double JOBINTERVALSTDDEV = 0.3 * JOBINTERVAL;

    GaussianSource jobIntervalGenerator = new GaussianSource(JOBINTERVAL,
            JOBINTERVALSTDDEV);

    private void scheduleJobOnWorker() {
        int bestWorker = -1;
        double bestCompletionTime = Double.POSITIVE_INFINITY;

        for (int i = 0; i < workerEstimators.length; i++) {
            final WorkerEstimator e = workerEstimators[i];
            final double t = e.estimateCompletionTime();
            if (t < bestCompletionTime) {
                bestWorker = i;
                bestCompletionTime = t;
            }
        }
        final WorkerEstimator est = workerEstimators[bestWorker];
        est.registerQueuedJob();
        workers[bestWorker].executeJob(est);

    }

    private void dispatchAJob() {
        scheduleJobOnWorker();
        final double interval = jobIntervalGenerator.next();
        // Schedule the arrival of a new event.
        final JobArrivedEvent e = new JobArrivedEvent(interval);
        q.addEvent(e);
    }

    private void runExperiment(final PrintStream stream, final String lbl,
            final boolean verbose, final boolean printEndStats,
            final double fast, final double normal, final double slow,
            final double stddev, final int jobCount) {
        // First, create some workers and worker estimators.
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
        int finishedJobCount = 0;

        dispatchAJob();
        while (finishedJobCount < jobCount) {
            final Event e = q.getNextEvent();

            now = e.getTime();
            if (e instanceof JobArrivedEvent) {
                dispatchAJob();
            } else if (e instanceof JobCompletedEvent) {
                final JobCompletedEvent ae = (JobCompletedEvent) e;
                final WorkerEstimator est = ae.worker;
                est.registerCompletedJob(ae.completionTime);
                finishedJobCount++;
            }
        }
        if (printEndStats) {
            for (int i = 0; i < workerEstimators.length; i++) {
                workerEstimators[i].printStatistics(System.out,
                        Integer.toString(i));
            }
            System.out.format(
                    "Fast: %3g normal: %3g slow: %3g  Average cost: %.3g\n",
                    fast, normal, slow, now / jobCount);
        } else {
            stream.println(lbl + " " + now / jobCount);
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

    private void runNormalSlowdownExperiments() {
        final double normalValues[] = { 110, 150, 200, 500, 1000, 2000, 5000,
                10000 };
        final double fast = 100;
        final double slowFactor = 1;
        PrintStream stream;
        final String fnm = WorkerEstimator.getName() + "-normal-slowdown.data";
        stream = openPrintFile(fnm);

        for (final double normal : normalValues) {
            final double slow = slowFactor * normal;

            for (int sample = 0; sample < SAMPLES; sample++) {
                final String lbl = Double.toString(normal);
                runExperiment(stream, lbl, false, false, fast, normal, slow,
                        STDDEV, JOBCOUNT);
            }
        }
        stream.close();
        System.out.println("Wrote file '" + fnm + "'");
    }

    private void runStdDevExperiments() {
        final double stddevs[] = { 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10 };
        final double fast = 100;
        final double normal = 5 * fast;
        final double slow = normal;
        PrintStream stream;
        final String fnm = WorkerEstimator.getName() + "-stddev.data";
        stream = openPrintFile(fnm);

        for (final double s : stddevs) {

            for (int sample = 0; sample < SAMPLES; sample++) {
                final String lbl = Double.toString(s * normal);
                runExperiment(stream, lbl, false, false, fast, normal, slow, s,
                        JOBCOUNT);
            }
        }
        stream.close();
        System.out.println("Wrote file '" + fnm + "'");
    }

    private void runSampleCountExperiments() {
        final int samples[] = { 100, 200, 500, 1000, 2000, 5000, 10000, 20000,
                50000, 100000 };
        final double fast = 100;
        final double normal = 10 * fast;
        final double slow = normal;
        PrintStream stream;
        final String fnm = WorkerEstimator.getName() + "-samplecount.data";
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
        final StochasticLearningMaster m = new StochasticLearningMaster(true);
        m.runNormalSlowdownExperiments();
        m.runStdDevExperiments();
        m.runSampleCountExperiments();
    }
}

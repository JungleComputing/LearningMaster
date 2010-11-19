package ibis.learningmaster;

import ibis.learningmaster.EventQueue.Event;
import ibis.steel.Estimator;
import ibis.steel.ExponentialDecayEstimator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

class StochasticLearningMaster {
    private static final int WORKERS = 20;
    private static final int SAMPLES = 10;
    private static int JOBCOUNT = 30000;
    private static final double STDDEV = 0.20;
    private final EventQueue q;
    private double now = 0;
    private double totalCompletionTime = 0.0;
    private final boolean verbose;

    public StochasticLearningMaster(final boolean verbose) {
        q = new EventQueue(verbose);
        this.verbose = verbose;
    }

    private static class JobCompletedEvent extends EventQueue.Event {
        final WorkerEstimator worker;
        final double completionTime;

        public JobCompletedEvent(final double t, final double completionTime,
                final WorkerEstimator worker) {
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
        JobArrivedEvent(final double time) {
            super(time);
        }

        @Override
        public String toString() {
            return "JobArrivedEvent[when=" + super.time + "]";
        }
    }

    private class Worker {
        ZeroClampedGaussianSource workTimeGenerator;
        private double busyUntilTime;
        private final String label;
        private double totalIdleTime = 0.0;

        Worker(final double average, final double stdDev, final String label) {
            this.label = label;
            workTimeGenerator = new ZeroClampedGaussianSource(average, stdDev);
        }

        /**
         * Execute a job on this worker. We place an event on the event queue to
         * mark the completion of this job.
         */
        @SuppressWarnings("synthetic-access")
        void executeJob(final WorkerEstimator est) {
            final double duration = workTimeGenerator.next();
            final double startTime;

            if (now > busyUntilTime) {
                // The worker currently isn't busy.
                startTime = now;
                totalIdleTime += now - busyUntilTime;
            } else {
                // The worker is busy; execution will be delayed.
                startTime = busyUntilTime;
            }
            final double completionTime = startTime + duration;
            busyUntilTime = completionTime;
            totalCompletionTime += completionTime - now;
            if (verbose) {
                System.out.println("New job for worker " + label
                        + ": startTime=" + startTime + " duration=" + duration
                        + " completionTime=" + completionTime);
            }
            final JobCompletedEvent e = new JobCompletedEvent(completionTime,
                    duration, est);
            q.addEvent(e);
        }

        @Override
        public String toString() {
            return label;
        }

        void printStatistics(final PrintStream s, final double totalTime) {
            totalIdleTime += now - busyUntilTime;
            final long perc = Math.round(100 * totalIdleTime / totalTime);
            s.format("%-4s: %s total idle time: %s (%d%%)\n", label,
                    workTimeGenerator.toString(),
                    Utils.formatSeconds(totalIdleTime), perc);
        }
    }

    private static class WorkerEstimator {
        private final Estimator performance = buildEstimator();
        private int queueLength = 0;
        private final String label;
        private double queueTime = 0.0;
        private double lastQueueEvent = 0;
        private double lastJobStart = 0;
        private int maxQueueLength = 0;

        static Estimator buildEstimator() {
            return new ExponentialDecayEstimator(0.1);
            // return new GaussianEstimator();
        }

        WorkerEstimator(final String label) {
            // Start with a very optimistic estimate to avoid corner cases
            this.label = label;
            performance.addSample(0);
        }

        @Override
        public String toString() {
            return label;
        }

        void printStatistics(final PrintStream s, final double totalTime) {
            final double qf = queueTime / totalTime;
            performance
                    .printStatistics(s, performance.getName() + ": " + label);
            s.println(label + ": maxQLen=" + maxQueueLength + " qf=" + qf);
        }

        static String getName() {
            return buildEstimator().getName();
        }

        double estimateCompletionTime(final double now) {
            double t = 0;
            if (queueLength > 0) {
                final double pessimisticEstimate = performance
                        .getHighEstimate();
                t = queueLength * pessimisticEstimate
                        * Math.pow(1.05, queueLength);
                // Take into account the time that
                // The job is already running.
                t -= now - lastJobStart;
            }
            // Also add the estimated time for the new job.
            // Don't let a negative value from the estimator
            // make a mess of things.
            t = Math.max(0, t + performance.getLikelyValue());
            return t;
        }

        void registerQueuedJob(final double now) {
            queueTime += queueLength * (now - lastQueueEvent);
            lastQueueEvent = now;
            lastJobStart = now;
            queueLength++;
            if (queueLength > maxQueueLength) {
                maxQueueLength = queueLength;
            }
        }

        void registerCompletedJob(final double completionTime, final double now) {
            queueTime += queueLength * (now - lastQueueEvent);
            lastQueueEvent = now;
            queueLength--;
            performance.addSample(completionTime);
        }
    }

    private static final Worker workers[] = new Worker[WORKERS];
    private static final WorkerEstimator workerEstimators[] = new WorkerEstimator[WORKERS];

    private void scheduleJobOnWorker() {
        int bestWorker = -1;
        double bestCompletionTime = Double.POSITIVE_INFINITY;

        for (int i = 0; i < workerEstimators.length; i++) {
            final WorkerEstimator e = workerEstimators[i];
            final double t = e.estimateCompletionTime(now);
            if (false) {
                System.out.print("Estimated completion time on " + e.label
                        + ": " + t + " Queue length=" + e.queueLength
                        + " stats: ");
                e.printStatistics(System.out, 0);
            }
            if (t < bestCompletionTime) {
                bestWorker = i;
                bestCompletionTime = t;
            }
        }
        if (bestWorker < 0) {
            System.out
                    .println("No reasonable worker found; volunteering worker 0");
            bestWorker = 0;
        }
        final WorkerEstimator est = workerEstimators[bestWorker];
        // System.out.println("Best Worker: " + est.label);
        est.registerQueuedJob(now);
        workers[bestWorker].executeJob(est);
    }

    private void runExperiment(final PrintStream stream, final String lbl,
            final boolean printEndStats, final double arrivalRate,
            final double arrivalStdDev, final double fast, final double slow,
            final double stddev, final int jobCount) {
        final ZeroClampedGaussianSource jobIntervalGenerator = new ZeroClampedGaussianSource(
                arrivalRate, arrivalStdDev);
        int submittedJobCount = 0;
        double totalExecutionTime = 0;
        // First, create some workers and worker estimators.
        for (int i = 0; i < WORKERS; i++) {
            final double d = slow - fast;
            final double v = fast + (double) i / (WORKERS - 1) * d;
            workers[i] = new Worker(v, stddev * v, "W" + i);
            workerEstimators[i] = new WorkerEstimator("W" + i);
        }
        int finishedJobCount = 0;

        scheduleJobOnWorker();
        final double interval = jobIntervalGenerator.next();
        // Schedule the arrival of a new event.
        final JobArrivedEvent e1 = new JobArrivedEvent(interval);
        q.addEvent(e1);
        submittedJobCount++;
        while (finishedJobCount < jobCount) {
            final Event e = q.getNextEvent();

            now = e.getTime();
            if (verbose) {
                System.out.println("Finished jobs: " + finishedJobCount);
            }
            if (e instanceof JobArrivedEvent) {
                scheduleJobOnWorker();
                if (submittedJobCount < jobCount) {
                    final double jobArrivalInterval = jobIntervalGenerator
                            .next();
                    // Schedule the arrival of a new event.
                    final JobArrivedEvent e2 = new JobArrivedEvent(now
                            + jobArrivalInterval);
                    q.addEvent(e2);
                    submittedJobCount++;
                }
            } else if (e instanceof JobCompletedEvent) {
                final JobCompletedEvent ae = (JobCompletedEvent) e;
                final WorkerEstimator est = ae.worker;
                est.registerCompletedJob(ae.completionTime, now);
                totalExecutionTime += ae.completionTime;
                finishedJobCount++;
            }
        }
        if (printEndStats) {
            for (int i = 0; i < workerEstimators.length; i++) {
                workers[i].printStatistics(stream, now);
                workerEstimators[i].printStatistics(stream, now);
            }
            System.out
                    .format("Fast: %3g slow: %3g  run time=%.3f average execution time: %3g average completion time: %3g\n",
                            fast, slow, now, (totalExecutionTime / jobCount),
                            (totalCompletionTime / jobCount));
        } else {
            stream.println(lbl + " " + totalExecutionTime / jobCount);
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
                runExperiment(stream, lbl, false, 0.9 * fast, STDDEV, fast,
                        slow, STDDEV, JOBCOUNT);
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
                runExperiment(stream, lbl, false, fast * 0.9, STDDEV, fast,
                        slow, s, JOBCOUNT);
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
                runExperiment(stream, lbl, false, 0.9 * fast, STDDEV, fast,
                        slow, STDDEV, jobCount);
            }
        }
        stream.close();
        System.out.println("Wrote file '" + fnm + "'");
    }

    public static void main(final String args[]) {
        final StochasticLearningMaster m = new StochasticLearningMaster(false);
        if (false) {
            m.runNormalSlowdownExperiments();
            m.runStdDevExperiments();
            m.runSampleCountExperiments();
        } else {
            m.runExperiment(System.out, "test", true, 0.2, 0.125, 1, 20, 0.4,
                    200000);
        }
    }
}

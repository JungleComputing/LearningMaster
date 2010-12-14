package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;
import ibis.steel.Estimate;
import ibis.steel.Estimator;
import ibis.steel.ExponentialDecayLogEstimator;
import ibis.steel.LogGaussianEstimate;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Schedule jobs one by one on the available workers. Note that there is always
 * at most one outstanding job. This is not realistic, but for the purposes of
 * learning behavior it is simpler.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class LearningScheduler implements Scheduler {
    private final ArrayList<WorkerInfo> workers = new ArrayList<WorkerInfo>();
    private final LinkedList<JobInstance> jobQueue = new LinkedList<JobInstance>();
    private static final double DECAY_FACTOR = 0.1;
    private static final int MAXIMAL_OUTSTANDING_JOBS = 2;

    private static class WorkerInfo {
        final IbisIdentifier node;
        final Estimator workTimeEstimator;
        boolean deleted = false;

        WorkerInfo(final IbisIdentifier node) {
            super();
            this.node = node;
            // TODO: better initial estimate.
            final Estimate est = new LogGaussianEstimate(Math.log(1e-4),
                    Math.log(1000), 1);
            workTimeEstimator = new ExponentialDecayLogEstimator(est,
                    DECAY_FACTOR);
        }

        void setDeleted() {
            deleted = true;
        }
    }

    private static int searchWorkerInfoList(final List<WorkerInfo> l,
            final IbisIdentifier node) {
        for (int i = 0; i < l.size(); i++) {
            if (node.equals(l.get(i).node)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void shutdown() {
        // Ignore.
    }

    /**
     * Removes the given worker from our list of workers.
     */
    @Override
    public void removeNode(final IbisIdentifier worker) {
        final int ix = searchWorkerInfoList(workers, worker);
        if (ix >= 0) {
            final WorkerInfo info = workers.remove(ix);
            if (info != null) {
                info.setDeleted();
            }
        }
    }

    @Override
    public void dumpState() {
        Globals.log.reportProgress("RoundRobinScheduler: workers="
                + workers.toString());
    }

    /**
     * Adds the given worker to our list of workers.
     * 
     * @param worker
     *            The worker to add.
     */
    @Override
    public void workerHasJoined(final IbisIdentifier worker) {
        workers.add(new WorkerInfo(worker));
    }

    @Override
    public void printStatistics(final PrintStream printStream) {
    }

    @Override
    public boolean shouldStop() {
        return jobQueue.isEmpty();
    }

    @Override
    public void returnJob(final JobInstance job) {
        jobQueue.add(job);
    }

    private WorkerInfo selectBestWorker(
            final WorkerAdministration workerAdministration) {
        WorkerInfo bestWorker = null;
        double bestResult = Double.POSITIVE_INFINITY;
        for (final WorkerInfo p : workers) {
            if (workerAdministration.hasRoomForJob(p.node,
                    MAXIMAL_OUTSTANDING_JOBS)) {
                final Estimate est = p.workTimeEstimator.getEstimate();
                final double v = est.getLikelyValue();
                if (v < bestResult) {
                    bestResult = v;
                    bestWorker = p;
                }
            }
        }
        return bestWorker;
    }

    @Override
    public boolean maintainOutstandingRequests(final Transmitter transmitter,
            final WorkerAdministration workerAdministration) {
        if (jobQueue.isEmpty()) {
            // There are no jobs to submit.
            return false;
        }
        if (workers.isEmpty()) {
            // There are no workers to submit jobs to.
            return false;
        }
        final WorkerInfo worker = selectBestWorker(workerAdministration);
        if (worker == null) {
            return false;
        }
        final JobInstance job = jobQueue.removeFirst();
        final int id = workerAdministration.addRequest(worker.node, job);
        // FIXME: properly handle job input
        final ExecuteJobMessage rq = new ExecuteJobMessage(job.job, id,
                job.input);
        transmitter.addToRequestQueue(worker.node, rq);
        return true;
    }

    @Override
    public boolean thereAreRequestsToSubmit() {
        return !jobQueue.isEmpty();
    }

    @Override
    public void submitRequest(final AtomicJob job, final Serializable input) {
        jobQueue.add(new JobInstance(job, input));
    }

}

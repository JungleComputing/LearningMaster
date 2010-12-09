package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;
import ibis.steel.Estimate;
import ibis.steel.Estimator;
import ibis.steel.ExponentialDecayLogEstimator;
import ibis.steel.LogGaussianEstimate;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Schedule tasks one by one on the available workers. Note that there is always
 * at most one outstanding task. This is not realistic, but for the purposes of
 * learning behavior it is simpler.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class LearningScheduler implements Scheduler {
    private final ArrayList<PeerInfo> peers = new ArrayList<PeerInfo>();
    private final LinkedList<Job> jobQueue = new LinkedList<Job>();
    private static final double DECAY_FACTOR = 0.1;
    private static final int MAXIMAL_OUTSTANDING_JOBS = 2;

    private static class PeerInfo {
        private boolean deleted = false;
        final IbisIdentifier node;
        final Estimator workTimeEstimator;
        private int outstandingJobs = 0;

        int getOutstandingJobs() {
            return outstandingJobs;
        }

        boolean isDeleted() {
            return deleted;
        }

        PeerInfo(final IbisIdentifier node) {
            super();
            this.node = node;
            // TODO: better initial estimate.
            final Estimate est = new LogGaussianEstimate(Math.log(1e-4),
                    Math.log(1000), 1);
            workTimeEstimator = new ExponentialDecayLogEstimator(est,
                    DECAY_FACTOR);
        }

        void setDeleted() {
            outstandingJobs = 0;
            deleted = true;
        }

        void registerOutstandingJob() {
            outstandingJobs++;
        }
    }

    private static int searchPeerInfoList(final List<PeerInfo> l,
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
     * Removes the given peer from our list of workers.
     */
    @Override
    public void removePeer(final IbisIdentifier peer) {
        final int ix = searchPeerInfoList(peers, peer);
        if (ix >= 0) {
            final PeerInfo info = peers.remove(ix);
            if (info != null) {
                info.setDeleted();
            }
        }
    }

    @Override
    public void dumpState() {
        Globals.log.reportProgress("RoundRobinScheduler: peers="
                + peers.toString());
    }

    /**
     * Adds the given peer to our list of workers.
     * 
     * @param peer
     *            The worker to add.
     */
    @Override
    public void workerHasJoined(final IbisIdentifier peer) {
        peers.add(new PeerInfo(peer));
    }

    @Override
    public void printStatistics(final PrintStream printStream) {
    }

    @Override
    public boolean shouldStop() {
        return jobQueue.isEmpty();
    }

    @Override
    public void returnTask(final Job job) {
        jobQueue.add(job);
    }

    @Override
    public boolean maintainOutstandingRequests(final Transmitter transmitter,
            final OutstandingRequestList outstandingRequests) {
        if (jobQueue.isEmpty()) {
            // There are no tasks to submit.
            return false;
        }
        if (peers.isEmpty()) {
            // There are no peers to submit tasks to.
            return false;
        }
        final Job job = jobQueue.removeFirst();
        final PeerInfo worker = selectBestPeer();
        if (worker == null) {
            return false;
        }
        final int id = outstandingRequests.add(worker.node, job);
        // FIXME: properly handle task input
        final ExecuteTaskMessage rq = new ExecuteTaskMessage(job, id, null);
        transmitter.addToRequestQueue(worker.node, rq);
        worker.registerOutstandingJob();
        return true;
    }

    private PeerInfo selectBestPeer() {
        PeerInfo bestWorker = null;
        double bestResult = Double.POSITIVE_INFINITY;
        for (final PeerInfo p : peers) {
            if (!p.isDeleted()
                    && p.getOutstandingJobs() < MAXIMAL_OUTSTANDING_JOBS) {
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
    public boolean thereAreRequestsToSubmit() {
        return !jobQueue.isEmpty();
    }

    @Override
    public void submitRequest(final AtomicJob job) {
        jobQueue.add(job);
    }

}

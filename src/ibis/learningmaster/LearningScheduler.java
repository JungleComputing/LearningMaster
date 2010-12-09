package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;

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

    private static class PeerInfo {
        private boolean deleted = false;
        final IbisIdentifier node;

        PeerInfo(IbisIdentifier node) {
            super();
            this.node = node;
        }

        void setDeleted() {
            deleted = true;
        }

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
        peers.remove(peer);
        final PeerInfo info = peerStats.get(peer);
        if (info != null) {
            info.setDeleted();
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
        return true;
    }

    @Override
    public void returnTask(final Job job) {
        jobQueue.add(job);
    }

    @Override
    public boolean maintainOutstandingRequests(final Transmitter transmitter,
            OutstandingRequestList outstandingRequests) {
        if (jobQueue.isEmpty()) {
            // There are no tasks to submit.
            return false;
        }
        if (peers.isEmpty()) {
            // There are no peers to submit tasks to.
            return false;
        }
        final Job job = jobQueue.removeFirst();
        final IbisIdentifier worker = selectBestPeer();
        if (worker == null) {
            return false;
        }
        final int id = outstandingRequests.add(worker, job);
        final ExecuteTaskMessage rq = new ExecuteTaskMessage(job, id);
        transmitter.addToRequestQueue(worker, rq);
        return true;
    }

    private PeerInfofier selectBestPeer() {
        for (final PeerInfo p : peers) {

        }
        return null;
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

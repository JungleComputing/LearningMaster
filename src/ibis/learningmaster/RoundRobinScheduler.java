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
class RoundRobinScheduler implements Scheduler {
    private final ArrayList<IbisIdentifier> peers = new ArrayList<IbisIdentifier>();
    private int nextPeer = 0;
    private final OutstandingRequestList outstandingRequests = new OutstandingRequestList();
    private final LinkedList<Job> jobQueue = new LinkedList<Job>();

    RoundRobinScheduler() {
    }

    @Override
    public void shutdown() {
    }

    /**
     * Removes the given peer from our list of workers.
     */
    @Override
    public void removePeer(final IbisIdentifier peer) {
        outstandingRequests.removePeer(peer, this);
        peers.remove(peer);
    }

    @Override
    public void dumpState() {
        Globals.log.reportProgress("RoundRobinScheduler: peers="
                + peers.toString());
        outstandingRequests.dumpState();
    }

    /**
     * Adds the given peer to our list of workers.
     * 
     * @param peer
     *            The worker to add.
     */
    @Override
    public void workerHasJoined(final IbisIdentifier peer) {
        peers.add(peer);
    }

    @Override
    public void registerCompletedTask(final Job job) {
        outstandingRequests.removeTask(job);
    }

    @Override
    public void printStatistics(final PrintStream printStream) {
    }

    @Override
    public boolean shouldStop() {
        return outstandingRequests.isEmpty() && outstandingRequests.isEmpty();
    }

    @Override
    public void returnTask(final Job id) {
        // FIXME: do this properly.
        outstandingRequests.add(0, id);
        final boolean removed = outstandingRequests.removeTask(id);
        if (!removed) {
            Globals.log.reportInternalError("Task " + id
                    + " was returned, but was not outstanding");
        }
    }

    @Override
    public boolean maintainOutstandingRequests(final Transmitter transmitter) {
        if (!outstandingRequests.isEmpty()) {
            // There already is an outstanding request.
            return false;
        }
        if (jobQueue.isEmpty()) {
            // No more tasks to submit.
            return false;
        }
        if (peers.isEmpty()) {
            // There are no peers to submit tasks to.
            return false;
        }
        final Job job = jobQueue.removeFirst();
        if (nextPeer >= peers.size()) {
            nextPeer = 0;
        }
        final IbisIdentifier worker = peers.get(nextPeer);
        nextPeer++;
        outstandingRequests.add(worker, job);
        final ExecuteTaskMessage rq = new ExecuteTaskMessage(job);
        transmitter.addToRequestQueue(worker, rq);
        return true;
    }

    @Override
    public boolean thereAreRequestsToSubmit() {
        return outstandingRequests.isEmpty() && !jobQueue.isEmpty();
    }

    @Override
    public void submitRequest(final AtomicJob job) {
        jobQueue.add(job);
    }

}

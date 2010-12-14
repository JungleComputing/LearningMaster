package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.io.Serializable;
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
    private final LinkedList<JobInstance> jobQueue = new LinkedList<JobInstance>();

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
        peers.add(peer);
    }

    @Override
    public void printStatistics(final PrintStream printStream) {
    }

    @Override
    public boolean shouldStop() {
        return jobQueue.isEmpty();
    }

    @Override
    public void returnTask(final JobInstance job) {
        jobQueue.add(job);
    }

    @Override
    public boolean maintainOutstandingRequests(final Transmitter transmitter,
            final WorkerAdministration outstandingRequests) {
        if (jobQueue.isEmpty()) {
            // There are no tasks to submit.
            return false;
        }
        if (peers.isEmpty()) {
            // There are no peers to submit tasks to.
            return false;
        }
        if (nextPeer >= peers.size()) {
            nextPeer = 0;
        }
        final IbisIdentifier worker = peers.get(nextPeer);
        nextPeer++;
        final JobInstance job = jobQueue.removeFirst();
        final int id = outstandingRequests.addRequest(worker, job);
        final ExecuteTaskMessage rq = new ExecuteTaskMessage(job.job, id,
                job.input);
        transmitter.addToRequestQueue(worker, rq);
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

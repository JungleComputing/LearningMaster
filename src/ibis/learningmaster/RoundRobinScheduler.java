package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Schedule jobs one by one on the available workers. Note that there is always
 * at most one outstanding job. This is not realistic, but for the purposes of
 * learning behavior it is simpler.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class RoundRobinScheduler implements Scheduler {
    private final ArrayList<IbisIdentifier> workers = new ArrayList<IbisIdentifier>();
    private int nextWorker = 0;
    private final LinkedList<JobInstance> jobQueue = new LinkedList<JobInstance>();

    @Override
    public void shutdown() {
        // Ignore.
    }

    /**
     * Removes the given worker from our list of workers.
     */
    @Override
    public void removeNode(final IbisIdentifier worker) {
        workers.remove(worker);
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
        workers.add(worker);
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

    @Override
    public boolean maintainOutstandingRequests(final Transmitter transmitter,
            final WorkerAdministration outstandingRequests) {
        if (jobQueue.isEmpty()) {
            // There are no jobs to submit.
            return false;
        }
        if (workers.isEmpty()) {
            // There are no workers to submit jobs to.
            return false;
        }
        if (nextWorker >= workers.size()) {
            nextWorker = 0;
        }
        final IbisIdentifier worker = workers.get(nextWorker);
        nextWorker++;
        final JobInstance job = jobQueue.removeFirst();
        final int id = outstandingRequests.addRequest(worker, job);
        final ExecuteJobMessage rq = new ExecuteJobMessage(job.job, id,
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

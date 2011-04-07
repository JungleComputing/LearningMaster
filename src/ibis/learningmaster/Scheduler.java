package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.io.Serializable;

interface Scheduler {
    void shutdown();

    void removeNode(IbisIdentifier worker);

    void dumpState();

    void workerHasJoined(IbisIdentifier source);

    void printStatistics(PrintStream printStream);

    /**
     * Returns true iff the entire master/worker engine should stop.
     * 
     * @return <code>true</code> iff the engine should stop.
     */
    boolean shouldStop();

    /**
     * Given a job instance <code>j</code>, returns it to the scheduler. This
     * method is typically used to recover jobs that were supposed to be
     * executed on workers that are now dead.
     * 
     * @param j
     *            The job to return to the scheduler.
     */
    void returnJob(JobInstance j);

    /**
     * Returns <code>true</code> iff there are currently requests waiting for
     * submission to the workers.
     * 
     * @return <code>true</code> if there are waiting requests.
     */
    boolean thereAreRequestsToSubmit();

    boolean maintainOutstandingRequests(Transmitter transmitter,
            WorkerAdministration outstandingRequests);

    void submitRequest(AtomicJob job, Serializable input);
}

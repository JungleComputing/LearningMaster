package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.io.Serializable;

interface Scheduler {
    void shutdown();

    void removePeer(IbisIdentifier peer);

    void dumpState();

    void workerHasJoined(IbisIdentifier source);

    void printStatistics(PrintStream printStream);

    /**
     * Returns true iff the engine should stop.
     * 
     * @return <code>true</code> iff the engine should stop.
     */
    boolean shouldStop();

    void returnTask(JobInstance j);

    boolean thereAreRequestsToSubmit();

    boolean maintainOutstandingRequests(Transmitter transmitter,
            WorkerAdministration outstandingRequests);

    void submitRequest(AtomicJob job, Serializable input);
}

package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;

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

    void returnTask(Job id);

    boolean thereAreRequestsToSubmit();

    boolean maintainOutstandingRequests(Transmitter transmitter,
            OutstandingRequestList outstandingRequests);

    void submitRequest(AtomicJob job);
}

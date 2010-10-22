package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;

interface Scheduler {
    void shutdown();

    void removePeer(IbisIdentifier peer);

    void dumpState();

    void peerHasJoined(IbisIdentifier source);

    void registerCompletedTask(int task);

    void printStatistics(PrintStream printStream);

    /**
     * Returns true iff the engine should stop.
     * 
     * @return <code>true</code> iff the engine should stop.
     */
    boolean shouldStop();

    void returnTask(int id);

    boolean maintainOutstandingRequests(
            OutstandingRequestList outstandingRequests, Transmitter transmitter);

    boolean requestsToSubmit();
}

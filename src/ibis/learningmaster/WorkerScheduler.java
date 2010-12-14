package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.io.Serializable;

/**
 * @author Kees van Reeuwijk
 * 
 */
class WorkerScheduler implements Scheduler {
    private boolean masterHasGone = false;
    private final IbisIdentifier master;

    WorkerScheduler(final IbisIdentifier master) {
        this.master = master;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void removePeer(final IbisIdentifier peer) {
        if (peer.equals(master)) {
            masterHasGone = true;
        }
    }

    @Override
    public void dumpState() {
        Globals.log.reportProgress("master=" + master + " masterHasGone="
                + masterHasGone);
    }

    @Override
    public void workerHasJoined(final IbisIdentifier source) {
        // Ignore
    }

    @Override
    public void printStatistics(final PrintStream printStream) {
    }

    @Override
    public boolean shouldStop() {
        return masterHasGone;
    }

    @Override
    public void returnTask(final JobInstance id) {
        Globals.log
                .reportInternalError("Someone tried to return a task to the worker scheduler");
    }

    @Override
    public boolean maintainOutstandingRequests(final Transmitter transmitter,
            final WorkerAdministration outstandingRequests) {
        return false;
    }

    @Override
    public boolean thereAreRequestsToSubmit() {
        return false;
    }

    @Override
    public void submitRequest(final AtomicJob job, final Serializable input) {
        throw new Error("Internal error: submitting jobs to a worker scheduler");
    }

}

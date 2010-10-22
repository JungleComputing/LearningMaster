package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;

/**
 * @author Kees van Reeuwijk
 * 
 */
public class WorkerScheduler implements Scheduler {
    private boolean masterHasGone = false;
    private final IbisIdentifier master;

    WorkerScheduler(IbisIdentifier master) {
        this.master = master;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void removePeer(IbisIdentifier peer) {
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
    public void peerHasJoined(IbisIdentifier source) {
        // TODO Auto-generated method stub

    }

    @Override
    public void registerCompletedTask(int task) {
        Globals.log
                .reportInternalError("Someone tried to register a completed task to the worker scheduler");
    }

    @Override
    public void printStatistics(PrintStream printStream) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean shouldStop() {
        return masterHasGone;
    }

    @Override
    public void returnTask(int id) {
        Globals.log
                .reportInternalError("Someone tried to return a task to the worker scheduler");
    }

    @Override
    public boolean maintainOutstandingRequests(
            OutstandingRequestList outstandingRequests, Transmitter transmitter) {
        return false;
    }

    @Override
    public boolean requestsToSubmit() {
        return false;
    }

}

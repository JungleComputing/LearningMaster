package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;

class RoundRobinScheduler implements Scheduler {
    private PeerSet peers;

    @Override
    public void shutdown() {
    }

    @Override
    public void removePeer(IbisIdentifier peer) {
        peers.remove(peer);
    }

    @Override
    public void dumpState() {
        Globals.log.reportProgress("RoundRobinScheduler: peers="
                + peers.toString());
    }

    @Override
    public void peerHasJoined(IbisIdentifier peer) {
        peers.add(peer);
    }

    @Override
    public void registerCompletedTask(int task) {
        // TODO Auto-generated method stub

    }

    @Override
    public void printStatistics(PrintStream printStream) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean shouldStop() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void returnTask(int id) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean maintainOutstandingRequests(
            OutstandingRequestList outstandingRequests, Transmitter transmitter) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean requestsToSubmit() {
        // TODO Auto-generated method stub
        return false;
    }

}

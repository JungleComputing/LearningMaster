package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.util.LinkedList;

class OutstandingRequestList {
    private static class OutstandingRequest {
        private int id;
        private IbisIdentifier worker;

        @Override
        public String toString() {
            return "workId=" + id + " worker=" + worker;
        }
    }

    private final LinkedList<OutstandingRequest> requests = new LinkedList<OutstandingRequestList.OutstandingRequest>();

    void dumpState() {
        Globals.log.reportProgress("There are " + requests.size()
                + " outstanding requests:");
        for (final OutstandingRequest r : requests) {
            Globals.log.reportProgress(" Request " + r);
        }
    }

    @SuppressWarnings("synthetic-access")
    void removePeer(IbisIdentifier peer, Scheduler scheduler) {
        for (final OutstandingRequest r : requests) {
            if (peer.equals(r.worker)) {
                Globals.log.reportProgress("Returning request " + r
                        + " to scheduler, since this worker has gone");
                scheduler.returnTask(r.id);
            }
        }
    }
}

package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.util.LinkedList;

class OutstandingRequestList {
    private final LinkedList<OutstandingRequest> requests = new LinkedList<OutstandingRequestList.OutstandingRequest>();
    private static int nextId = 0;

    private static class OutstandingRequest {
        private final Job job;
        private final int id;
        private final IbisIdentifier worker;

        public OutstandingRequest(final IbisIdentifier worker, final Job job,
                final int id) {
            this.job = job;
            this.id = id;
            this.worker = worker;
        }

        @Override
        public String toString() {
            return "workId=" + id + " worker=" + worker;
        }
    }

    int addRequest(final IbisIdentifier worker, final Job job) {
        final int id = nextId++;
        final OutstandingRequest rq = new OutstandingRequest(worker, job, id);
        requests.add(rq);
        return id;
    }

    void dumpState() {
        Globals.log.reportProgress("There are " + requests.size()
                + " outstanding requests:");
        for (final OutstandingRequest r : requests) {
            Globals.log.reportProgress(" Request " + r);
        }
    }

    @SuppressWarnings("synthetic-access")
    void removePeer(final IbisIdentifier peer, final Scheduler scheduler) {
        for (final OutstandingRequest r : requests) {
            if (peer.equals(r.worker)) {
                requests.remove(r);
                scheduler.returnTask(r.job);
                Globals.log.reportProgress("Returning request " + r
                        + " to scheduler, since this worker has gone");
            }
        }
    }

    boolean isEmpty() {
        return requests.isEmpty();
    }

    @SuppressWarnings("synthetic-access")
    boolean removeTask(final int id, final boolean failed) {
        if (failed) {
            Globals.log.reportError("Task  " + id + " failed");
        }
        for (final OutstandingRequest r : requests) {
            if (r.id == id) {
                Globals.log.reportProgress("Returning request " + r
                        + " to scheduler");
                requests.remove(r);
                return true;
            }
        }
        return false;
    }
}

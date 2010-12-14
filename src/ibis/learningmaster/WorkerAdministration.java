package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

class WorkerAdministration {
    private final ConcurrentHashMap<IbisIdentifier, WorkerInfo> workerInfo = new ConcurrentHashMap<IbisIdentifier, WorkerAdministration.WorkerInfo>();
    private int outstandingRequests = 0;
    private static int nextId = 0;

    private static final class WorkerInfo {
        private final ArrayList<OutstandingRequest> requests = new ArrayList<OutstandingRequest>();
        private boolean deleted = false;

        synchronized boolean add(final OutstandingRequest rq) {
            requests.add(rq);
            return deleted;
        }

        synchronized void returnRequests(final Scheduler scheduler) {
            for (final OutstandingRequest r : requests) {
                requests.remove(r);
                scheduler.returnTask(r.job);
                Globals.log.reportProgress("Returning request " + r
                        + " to scheduler, since this worker has gone");
            }
        }

        synchronized void setDeleted() {
            deleted = true;
        }

        public void removeTask(final int id) {
            for (final OutstandingRequest r : requests) {
                if (r.id == id) {
                    Globals.log.reportProgress("Returning request " + r
                            + " to scheduler");
                    requests.remove(r);
                    return;
                }
            }
        }

    }

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
        final WorkerInfo info = workerInfo.get(worker);
        if (info == null) {
            Globals.log.reportInternalError("Ibis '" + worker
                    + "' not in administration");
            return -1;
        }
        final int id = nextId++;
        final OutstandingRequest rq = new OutstandingRequest(worker, job, id);
        info.add(rq);
        outstandingRequests++;
        return id;
    }

    void dumpState() {
    }

    @SuppressWarnings("synthetic-access")
    void removePeer(final IbisIdentifier peer, final Scheduler scheduler) {
        final WorkerInfo info = workerInfo.get(peer);
        if (info != null) {
            info.setDeleted();
            info.returnRequests(scheduler);
        }
    }

    boolean isEmpty() {
        return outstandingRequests > 0;
    }

    @SuppressWarnings("synthetic-access")
    boolean removeTask(final IbisIdentifier worker, final int id,
            final boolean failed) {
        outstandingRequests--;
        if (failed) {
            Globals.log.reportError("Task  " + id + " failed");
        }
        final WorkerInfo info = workerInfo.get(worker);
        if (info != null) {
            info.removeTask(id);
        }
        return false;
    }
}

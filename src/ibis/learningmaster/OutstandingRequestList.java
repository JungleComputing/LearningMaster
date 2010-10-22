package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.util.LinkedList;

class OutstandingRequestList {
	private final LinkedList<OutstandingRequest> requests = new LinkedList<OutstandingRequestList.OutstandingRequest>();

	private static class OutstandingRequest {
		private final int id;
		private final IbisIdentifier worker;

		public OutstandingRequest(final IbisIdentifier worker, final int id) {
			this.id = id;
			this.worker = worker;
		}

		@Override
		public String toString() {
			return "workId=" + id + " worker=" + worker;
		}
	}

	void add(final IbisIdentifier worker, final int id){
		final OutstandingRequest rq = new OutstandingRequest(worker, id);
		requests.add(rq);
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
				scheduler.returnTask(r.id);
				Globals.log.reportProgress("Returning request " + r
						+ " to scheduler, since this worker has gone");
			}
		}
	}

	boolean isEmpty() {
		return requests.isEmpty();
	}

	boolean removeTask(final int id) {
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

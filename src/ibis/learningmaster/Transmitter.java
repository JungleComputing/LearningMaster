package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;

class Transmitter extends Thread {
	private final SendQueue requestQueue = new SendQueue();
	private final SendQueue bookkeepingQueue = new SendQueue();
	private final SendQueue dataQueue = new SendQueue();
	private final SendQueue retryQueue = new SendQueue();
	private final PacketSendPort sendPort;
	private final EngineInterface engine;
	private final PeerSet deadPeers = new PeerSet();
	private long idleTime = 0L;
	private boolean sentMessages = false;
	private boolean stopped;
	private boolean shuttingDown = false;

	Transmitter(final EngineInterface node) {
		super("Arnold transmitter thread");
		setDaemon(true);
		this.sendPort = new PacketSendPort(node);
		this.engine = node; // We make sure we only access the engine interface.
		setPriority(Thread.NORM_PRIORITY + 1);
	}

	void addToRequestQueue(final IbisIdentifier destination,
			final SmallMessage msg) {
		if (Settings.TraceTransmitter) {
			Globals.log.reportProgress("Transmitter: put on request queue: "
					+ msg);
		}
		requestQueue.add(destination, msg);
		wakeTransmitter();
	}

	void addToDataQueue(final IbisIdentifier destination, final Message msg) {
		if (Settings.TraceTransmitter) {
			Globals.log
					.reportProgress("Transmitter: put on data queue: " + msg);
		}
		dataQueue.add(destination, msg);
		wakeTransmitter();
	}

	void addToBookkeepingQueue(final IbisIdentifier destination,
			final Message msg) {
		if (Settings.TraceTransmitter) {
			Globals.log
					.reportProgress("Transmitter: put on bookkeeping queue: "
							+ msg);
		}
		bookkeepingQueue.add(destination, msg);
		wakeTransmitter();
	}

	/**
	 * The interval between wakes in ms, if the transmitter isn't woken up for
	 * work.
	 */
	private static final long TRANSMITTER_WAKE_INTERVAL = 5000;

	/**
	 * The sleep interval if there are retry entries in the queue.
	 */
	private static final long RETRY_WAKE_INTERVAL = 100;

	/**
	 * Sends the given queued message.
	 * 
	 * @param qm
	 *            The queued message to send.
	 * @return <code>true</code> iff we added to the retry queue.
	 */
	private boolean sendMessage(final QueuedMessage qm) {
		if (deadPeers.contains(qm.destination)) {
			Globals.log
					.reportProgress("Transmitter dropped message to dead peer: "
							+ qm);
			return false;
		}
		if (Settings.TraceTransmitter) {
			Globals.log.reportProgress("Transmitter: sending: " + qm);
		}
		final Message msg = qm.msg;
		final boolean ok = sendPort.sendMessage(qm.destination, msg);
		if (!ok) {
			qm.retries++;
			if (qm.retries > Settings.MAXIMAL_SEND_RETRIES) {
				Globals.log.reportProgress("Dropped message after "
						+ Settings.MAXIMAL_SEND_RETRIES + " retries");
				engine.setSuspect(qm.destination);
			}
			retryQueue.add(qm);
			return true;
		}
		return false;
	}

	void deletePeer(final IbisIdentifier peer) {
		deadPeers.add(peer);
	}

	private synchronized void wakeTransmitter() {
		this.notifyAll();
	}

	private synchronized boolean isStopped() {
		return stopped;
	}

	@Override
	public void run() {
		while (!isStopped()) {
			boolean addedToRetriesQueue = false;
			while (true) {
				// Request messages have top priority.
				if (!requestQueue.isEmpty()) {
					final QueuedMessage msg = requestQueue.getNext();
					addedToRetriesQueue |= sendMessage(msg);
					sentMessages = true;
				} else if (!bookkeepingQueue.isEmpty()) {
					// Bookkeeping messages have priority over data
					// messages.
					final QueuedMessage msg = bookkeepingQueue.getNext();
					addedToRetriesQueue |= sendMessage(msg);
					sentMessages = true;
				} else if (!dataQueue.isEmpty()) {
					// Data messages have priority over retries.
					final QueuedMessage msg = dataQueue.getNext();
					addedToRetriesQueue |= sendMessage(msg);
					sentMessages = true;
				} else if (!addedToRetriesQueue && !retryQueue.isEmpty()) {
					// Retries have lowest priority.
					// To avoid being stuck in a retry loop, only
					// handle retry messages if no new ones have been
					// queued.
					final QueuedMessage msg = retryQueue.getNext();
					addedToRetriesQueue |= sendMessage(msg);
					sentMessages = true;
				} else {
					break;
				}
			}
			engine.wakeEngineThread();
			try {
				if (Settings.TraceTransmitterLoop) {
					Globals.log.reportProgress("Transmitter: waiting");
				}
				synchronized (this) {
					if (requestQueue.isEmpty() && bookkeepingQueue.isEmpty()
							&& dataQueue.isEmpty()) {
						final long startWaitTime = System.nanoTime();
						final long sleepTime = shuttingDown ? 1 : retryQueue
								.isEmpty() ? TRANSMITTER_WAKE_INTERVAL
								: RETRY_WAKE_INTERVAL;
						System.out.println("Transmitter: sleepTime="
								+ sleepTime);
						wait(sleepTime);
						if (sentMessages) {
							final long waitTime = System.nanoTime()
									- startWaitTime;
							idleTime += waitTime;
						}
					}
				}
			} catch (final InterruptedException e) {
				// ignore.
			}
			if (Settings.TraceTransmitterLoop) {
				Globals.log.reportProgress("Transmitter: looping");
			}
		}

	}

	boolean needsMoreData() {
		final int sz = dataQueue.size();
		return sz < Settings.IDEAL_TRANSMITTER_QUEUE_LENGTH;
	}

	private void printQueueStatistics(final PrintStream s) {
		requestQueue.printStatistics(s, "request queue");
		bookkeepingQueue.printStatistics(s, "bookkeeping queue");
		dataQueue.printStatistics(s, "data queue");
		retryQueue.printStatistics(s, "retry queue");
	}

	synchronized void dumpState() {
		Globals.log.reportProgress("Transmitter queues: data="
				+ dataQueue.size() + " bookkeeping=" + bookkeepingQueue.size()
				+ " requests=" + requestQueue.size() + " retries="
				+ retryQueue.size() + " transmitter idle time: "
				+ Utils.formatSeconds(idleTime * 1e-9));
		printQueueStatistics(Globals.log.getPrintStream());
	}

	synchronized void printStatistics(final PrintStream s) {
		sendPort.printStatistics(s, "transmitter");
		s.println("Transmitter idle time: "
				+ Utils.formatSeconds(idleTime * 1e-9));
		printQueueStatistics(s);
	}

	synchronized void setStopped() {
		stopped = true;
		this.notifyAll();
	}

	/**
	 * Remove any pending entries from the queue. Since this obviously involves
	 * unrecoverable loss of information, this method is only useful during
	 * shutdown of the peer.
	 */
	void setShuttingDown() {
		synchronized (this) {
			shuttingDown = true;
		}
		retryQueue.clear();
		dataQueue.clear();
		requestQueue.clear();
		bookkeepingQueue.clear();
	}
}

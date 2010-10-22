package ibis.learningmaster;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.Registry;
import ibis.ipl.RegistryEventHandler;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

class Engine extends Thread implements PacketReceiveListener,
		RegistryEventHandler, EngineInterface {
	private static final IbisCapabilities ibisCapabilities = new IbisCapabilities(
			IbisCapabilities.MEMBERSHIP_UNRELIABLE,
			IbisCapabilities.ELECTIONS_STRICT);
	private static final String MASTER_ELECTION_NAME = "master-election";
	private final ReceivedMessageQueue receivedMessageQueue = new ReceivedMessageQueue(
			Settings.MAXIMAL_RECEIVED_MESSAGE_QUEUE_LENGTH);
	private final TimeStatistics receivedMessageQueueStatistics = new TimeStatistics();
	private final Flag stopped = new Flag(false);
	private final Transmitter transmitter;
	private final ConcurrentLinkedQueue<IbisIdentifier> deletedPeers = new ConcurrentLinkedQueue<IbisIdentifier>();
	private final ConcurrentLinkedQueue<IbisIdentifier> newPeers = new ConcurrentLinkedQueue<IbisIdentifier>();
	private final ConcurrentLinkedQueue<RequestMessage> workQueue = new ConcurrentLinkedQueue<RequestMessage>();
	private final PacketUpcallReceivePort receivePort;
	final Ibis localIbis;
	private int activeWorkers = 0;
	private long receivedMessageHandlingTime = 0;
	private long requestsHandlingTime = 0;
	private long idleTime = 0;
	private final boolean isMaster;

	private final Scheduler scheduler;

	Engine(final boolean helper) throws IbisCreationFailedException,
			IOException {
		super("LearningMaster engine thread");
		final boolean runForMaster = !helper;
		this.transmitter = new Transmitter(this);
		final Properties ibisProperties = new Properties();
		this.localIbis = IbisFactory.createIbis(ibisCapabilities,
				ibisProperties, true, this, PacketSendPort.portType,
				PacketUpcallReceivePort.portType);
		final Registry registry = localIbis.registry();
		final IbisIdentifier myIbis = localIbis.identifier();
		final IbisIdentifier masterIdentifier = registry
				.elect(MASTER_ELECTION_NAME);
		isMaster = masterIdentifier.equals(myIbis);
		if (isMaster) {
			scheduler = new RoundRobinScheduler(Settings.TASK_COUNT);
		} else {
			scheduler = new WorkerScheduler(masterIdentifier);
		}
		receivePort = new PacketUpcallReceivePort(localIbis,
				Globals.receivePortName, this);
		System.out.println("runForSpecialNode=" + runForMaster);
		transmitter.start();
		registry.enableEvents();
		receivePort.enable();
		if (Settings.TraceNodeCreation) {
			Globals.log.reportProgress("Created ibis " + myIbis + " "
					+ Utils.getPlatformVersion() + " host "
					+ Utils.getCanonicalHostname());
			Globals.log.reportProgress(" isMaster=" + isMaster);
		}
	}

	@Override
	public void died(final IbisIdentifier peer) {
		if (peer.equals(localIbis.identifier())) {
			if (!stopped.isSet()) {
				Globals.log
						.reportError("This peer has been declared dead, we might as well stop");
				setStopped();
			}
		} else {
			transmitter.deletePeer(peer);
			deletedPeers.add(peer);
		}
		wakeEngineThread(); // Something interesting has happened.
	}

	@Override
	public void left(final IbisIdentifier peer) {
		if (peer.equals(localIbis.identifier())) {
			if (!stopped.isSet()) {
				Globals.log
						.reportError("This peer has been declared `left', we might as well stop");
				setStopped();
			}
		} else {
			transmitter.deletePeer(peer);
			deletedPeers.add(peer);
		}
		wakeEngineThread(); // Something interesting has happened.
	}

	@Override
	public void electionResult(final String arg0, final IbisIdentifier arg1) {
		// Not interesting.
	}

	@Override
	public void gotSignal(final String arg0, final IbisIdentifier arg1) {
		// Not interesting.
	}

	@Override
	public void joined(final IbisIdentifier peer) {
		newPeers.add(peer);
		if (Settings.TraceEngine) {
			Globals.log.reportProgress("New peer " + peer);
		}
		wakeEngineThread(); // Something interesting has happened.
	}

	private void registerNewPeer(final IbisIdentifier peer) {
		if (peer.equals(localIbis.identifier())) {
			// That's the local node. Ignore.
			return;
		}
		activeWorkers++;
		if (Settings.TracePeers) {
			Globals.log.reportProgress("Peer " + peer + " has joined");
		}
		// We only add it to the administration when it has sent
		// a join message.
	}

	private void registerPeerLeft(final IbisIdentifier peer) {
		if (Settings.TracePeers) {
			Globals.log.reportProgress("Peer " + peer + " has left");
		}
		activeWorkers--;
		scheduler.removePeer(peer);
	}

	/**
	 * Handle any new and deleted peers that have been registered after the last
	 * call.
	 * 
	 * @return <code>true</code> iff we made any changes to the node state.
	 */
	private boolean registerNewAndDeletedPeers() {
		boolean changes = false;
		while (true) {
			final IbisIdentifier peer = deletedPeers.poll();
			if (peer == null) {
				break;
			}
			registerPeerLeft(peer);
			changes = true;
		}
		while (true) {
			final IbisIdentifier peer = newPeers.poll();
			if (peer == null) {
				break;
			}
			registerNewPeer(peer);
			changes = true;
		}
		return changes;
	}

	/**
	 * Register the fact that no new peers can join this pool. Not relevant, so
	 * we ignore this.
	 */
	@Override
	public void poolClosed() {
		// Not interesting.
	}

	@Override
	public void poolTerminated(final IbisIdentifier arg0) {
		setStopped();
	}

	/**
	 * This ibis was reported as 'may be dead'. Try not to communicate with it.
	 * 
	 * @param peer
	 *            The peer that may be dead.
	 */
	@Override
	public void setSuspect(final IbisIdentifier peer) {
		if (Settings.TracePeers) {
			Globals.log.reportProgress(true, "Peer " + peer + " may be dead");
		}
		try {
			localIbis.registry().assumeDead(peer);
		} catch (final IOException e) {
			// Nothing we can do about it.
		}
	}

	private void setStopped() {
		stopped.set();
		wakeEngineThread(); // Something interesting has happened.
	}

	/**
	 * Handles an incoming message.
	 * 
	 * @param packet
	 *            The message to handle.
	 */
	@Override
	public void messageReceived(final Message packet) {
		// We are not allowed to do I/O in this thread, and we shouldn't
		// take too much time, so put all messages in a local queue to be
		// handled by the main loop.
		packet.arrivalTime = System.currentTimeMillis();
		receivedMessageQueue.add(packet);
		if (Settings.TraceReceiver) {
			Globals.log.reportProgress("Added to receive queue: " + packet);
		}
		wakeEngineThread(); // Something interesting has happened.
	}

	/** Tell the engine thread that something interesting has happened. */
	@Override
	public void wakeEngineThread() {
		synchronized (this) {
			this.notifyAll();
		}
	}

	/**
	 * Extracts all information from an incoming message, and update the
	 * administration.
	 * 
	 * @param msg
	 *            The incoming message.
	 */
	private void handleMessage(final Message msg) {
		if (msg instanceof RequestMessage) {
			final RequestMessage r = (RequestMessage) msg;
			workQueue.add(r);
		} else if (msg instanceof TaskCompletedMessage) {
			scheduler.registerCompletedTask(((TaskCompletedMessage) msg).task);
		} else if (msg instanceof RegisterWorkerMessage) {
			scheduler.peerHasJoined(((RegisterWorkerMessage) msg).source);
		} else {
			Globals.log.reportInternalError("Don't know how to handle a "
					+ msg.getClass() + " message");
		}
	}

	private boolean handleIncomingMessages() {
		final long start = System.nanoTime();
		boolean progress = false;
		while (true) {
			final Message msg = receivedMessageQueue.getNext();
			if (msg == null) {
				break;
			}
			final long lingerTime = System.currentTimeMillis()
					- msg.arrivalTime;
			receivedMessageQueueStatistics.registerSample(lingerTime * 1e-3);
			handleMessage(msg);
			progress = true;
		}
		final long duration = System.nanoTime() - start;
		receivedMessageHandlingTime += duration;
		return progress;
	}

	/**
	 * If the work queue is not empty, get a task request from the work queue
	 * and handle it.
	 * 
	 * @return <code>true</code> iff we actually handled a task request.
	 */
	private boolean handleAWorkRequest() {
		final RequestMessage request = workQueue.poll();

		if (request == null) {
			// No requests.
			return false;
		}
		final int jobNo = request.jobNo;
		try {
			Thread.sleep(Settings.TASK_DURATION);
		} catch (final InterruptedException e) {
			// Ignore
		}
		final Message msg = new TaskCompletedMessage(jobNo);
		transmitter.addToBookkeepingQueue(request.source, msg);
		return true;
	}

	/**
	 * Make sure there are enough outstanding requests.
	 */
	private boolean maintainOutstandingRequests() {
		final long start = System.nanoTime();
		final boolean progress = scheduler
				.maintainOutstandingRequests(transmitter);
		final long duration = System.nanoTime() - start;
		requestsHandlingTime += duration;
		return progress;
	}

	private synchronized void printStatistics(final PrintStream s) {
		s.println("message handling "
				+ Utils.formatSeconds(1e-9 * receivedMessageHandlingTime)
				+ " requests handling "
				+ Utils.formatSeconds(1e-9 * requestsHandlingTime) + " idle "
				+ Utils.formatSeconds(1e-3 * idleTime));
		receivedMessageQueueStatistics.printStatistics(s,
				"receive queue linger time");
	}

	private synchronized void dumpEngineState() {
		Globals.log.reportProgress("Main thread was only woken by timeout");
		receivedMessageQueue.dump();
		receivedMessageQueueStatistics.printStatistics(
				Globals.log.getPrintStream(), "receive queue linger time");
		Globals.log.reportProgress("Maximal receive queue length: "
				+ receivedMessageQueue.getMaximalQueueLength());
		Globals.log.reportProgress("message handling "
				+ Utils.formatSeconds(1e-9 * receivedMessageHandlingTime)
				+ " requests handling "
				+ Utils.formatSeconds(1e-9 * requestsHandlingTime) + " idle "
				+ Utils.formatSeconds(1e-3 * idleTime));
		Globals.log.reportProgress("Engine: " + activeWorkers + " active, "
				+ deletedPeers.size() + " deleted peers");
		transmitter.dumpState();
		scheduler.dumpState();
	}

	@Override
	public void run() {
		final int sleepTime = Settings.MAXIMAL_ENGINE_SLEEP_INTERVAL;
		try {
			while (!stopped.isSet()) {
				boolean sleptLong = false;
				boolean progress;
				do {
					// Keep doing bookkeeping chores until all is done.
					final boolean progressIncoming = handleIncomingMessages();
					final boolean progressPeerChurn = registerNewAndDeletedPeers();
					final boolean progressRequests = maintainOutstandingRequests();
					final boolean progressWork = handleAWorkRequest();
					progress = progressIncoming || progressPeerChurn
							|| progressRequests || progressWork;
					if (Settings.TraceDetailedProgress) {
						if (progress) {
							Globals.log.reportProgress("EE p=true i="
									+ progressIncoming + " c="
									+ progressPeerChurn + " r="
									+ progressRequests + " w=" + progressWork);
							if (progressIncoming) {
								receivedMessageQueue.printCounts();
							}
							if (progressRequests) {
								scheduler.dumpState();
							}
						}
					}
				} while (progress);
				synchronized (this) {
					final boolean messageQueueIsEmpty = receivedMessageQueue
							.isEmpty();
					final boolean noRequestsToSubmit = !scheduler
							.requestsToSubmit();
					if (!stopped.isSet() && messageQueueIsEmpty
							&& newPeers.isEmpty() && deletedPeers.isEmpty()
							&& noRequestsToSubmit && workQueue.isEmpty()) {
						try {
							final long sleepStartTime = System
									.currentTimeMillis();
							if (Settings.TraceEngine) {
								Globals.log
										.reportProgress("Main loop: waiting");
							}
							this.wait(sleepTime);
							final long sleepInterval = System
									.currentTimeMillis() - sleepStartTime;
							idleTime += sleepInterval;
							sleptLong = sleepInterval > 9 * sleepTime / 10;
						} catch (final InterruptedException e) {
							// Ignored.
						}
					}
				}
				if (sleptLong) {
					if (activeWorkers > 0) {
						dumpEngineState();
					}
				}
				if (scheduler.shouldStop()) {
					stopped.set();
				}
			}
		} finally {
			transmitter.setShuttingDown();
			scheduler.shutdown();
			try {
				transmitter.join(Settings.TRANSMITTER_SHUTDOWN_TIMEOUT);
			} catch (final InterruptedException e) {
				// ignore.
			}
			transmitter.setStopped();
			try {
				localIbis.end();
			} catch (final IOException x) {
				// Nothing we can do about it.
			}
		}
		printStatistics(Globals.log.getPrintStream());
		scheduler.printStatistics(Globals.log.getPrintStream());
		transmitter.printStatistics(Globals.log.getPrintStream());
		Utils.printThreadStats(Globals.log.getPrintStream());
	}
}

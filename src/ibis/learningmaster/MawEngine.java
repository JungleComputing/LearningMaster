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
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

class MawEngine extends Thread implements PacketReceiveListener,
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
    private final ConcurrentLinkedQueue<ExecuteTaskMessage> workQueue = new ConcurrentLinkedQueue<ExecuteTaskMessage>();
    private final PacketUpcallReceivePort receivePort;
    private final Ibis localIbis;
    private int activeWorkers = 0;
    private long receivedMessageHandlingTime = 0;
    private long requestsHandlingTime = 0;
    private long idleTime = 0;
    private final boolean isMaster;
    private final OutstandingRequestList outstandingRequests = new OutstandingRequestList();

    private final Scheduler scheduler;

    private static class SleepJob implements AtomicJob {

        @Override
        public boolean isSupported() {
            return true;
        }

        @Override
        public Serializable run(final Serializable input)
                throws JobFailedException {
            final Long time = (Long) input;
            try {
                Thread.sleep(time);
            } catch (final InterruptedException e) {
                // Ignore
            }
            return null;
        }

    }

    MawEngine(final boolean helper) throws IbisCreationFailedException,
            IOException {
        super("LearningMaster engine thread");
        final boolean runForMaster = !helper;
        transmitter = new Transmitter(this);
        final Properties ibisProperties = new Properties();
        localIbis = IbisFactory.createIbis(ibisCapabilities, ibisProperties,
                true, this, PacketSendPort.portType,
                PacketUpcallReceivePort.portType);
        final Registry registry = localIbis.registry();
        final IbisIdentifier myIbis = localIbis.identifier();
        final IbisIdentifier masterIdentifier = registry
                .elect(MASTER_ELECTION_NAME);
        isMaster = masterIdentifier.equals(myIbis);
        if (isMaster) {
            scheduler = new RoundRobinScheduler();
            for (int i = 0; i < Settings.TASK_COUNT; i++) {
                scheduler.submitRequest(new SleepJob());
            }
        } else {
            scheduler = new WorkerScheduler(masterIdentifier);
        }
        receivePort = new PacketUpcallReceivePort(localIbis,
                Globals.receivePortName, this);
        System.out.println("runForMaster=" + runForMaster);
        transmitter.start();
        registry.enableEvents();
        receivePort.enable();
        if (!isMaster) {
            // Tell the master we're ready.
            transmitter.addToBookkeepingQueue(masterIdentifier,
                    new RegisterWorkerMessage());
        } else {
            // FIXME: submit work.
        }
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
        outstandingRequests.removePeer(peer, scheduler);
    }

    /**
     * Handle any new and deleted peers that have been registered after the last
     * call.
     * 
     * @return <code>true</code> iff we made any changes to the node state.
     */
    private boolean registerNewAndDeletedNodes() {
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
        packet.arrivalTime = System.nanoTime();
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
            notifyAll();
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
        if (msg instanceof ExecuteTaskMessage) {
            final ExecuteTaskMessage r = (ExecuteTaskMessage) msg;
            workQueue.add(r);
        } else if (msg instanceof TaskCompletedMessage) {
            final TaskCompletedMessage taskCompletedMessage = (TaskCompletedMessage) msg;
            outstandingRequests.removeTask(taskCompletedMessage.task);
        } else if (msg instanceof RegisterWorkerMessage) {
            final RegisterWorkerMessage registerWorkerMessage = (RegisterWorkerMessage) msg;
            scheduler.workerHasJoined(registerWorkerMessage.source);
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
            Globals.log.reportProgress("Get incoming message from queue: "
                    + msg);
            if (msg == null) {
                break;
            }
            final long lingerTime = System.nanoTime() - msg.arrivalTime;
            receivedMessageQueueStatistics.registerSample(lingerTime * 1e-9);
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
        final ExecuteTaskMessage request = workQueue.poll();
        Exception failure = null;

        if (request == null) {
            if (Settings.TraceWorker) {
                Globals.log.reportProgress("Work queue is empty");
            }
            return false;
        }
        final Job job = request.job;
        if (Settings.TraceWorker) {
            Globals.log.reportProgress("Starting execution of job " + job);
        }
        final long startTime = System.nanoTime();
        Serializable res;
        try {
            if (job instanceof AtomicJob) {
                final AtomicJob aj = (AtomicJob) job;
                res = aj.run(request.input);
            } else {
                // FIXME: properly handle this.
                return false;
            }
        } catch (final JobFailedException x) {
            failure = x;
            res = null;
        }
        final long endTime = System.nanoTime();
        if (Settings.TraceWorker) {
            Globals.log.reportProgress("Ended execution of job " + job);
        }
        final Message msg = new TaskCompletedMessage(request.id, res,
                1e-9 * (endTime - startTime));
        transmitter.addToBookkeepingQueue(request.source, msg);
        return true;
    }

    /**
     * Make sure there are enough outstanding requests.
     */
    private boolean maintainOutstandingRequests() {
        final long start = System.nanoTime();
        final boolean progress = scheduler.maintainOutstandingRequests(
                transmitter, outstandingRequests);
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
        outstandingRequests.dumpState();
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
                    final boolean progressNodeChurn = registerNewAndDeletedNodes();
                    final boolean progressRequests = maintainOutstandingRequests();
                    final boolean progressWork = handleAWorkRequest();
                    progress = progressIncoming || progressNodeChurn
                            || progressRequests || progressWork;
                    if (Settings.TraceDetailedProgress) {
                        if (progress) {
                            Globals.log.reportProgress("EE p=true i="
                                    + progressIncoming + " c="
                                    + progressNodeChurn + " r="
                                    + progressRequests + " w=" + progressWork);
                            if (progressIncoming) {
                                receivedMessageQueue.printCounts();
                            }
                            if (progressRequests) {
                                scheduler.dumpState();
                                outstandingRequests.dumpState();
                            }
                        }
                    }
                } while (progress);
                synchronized (this) {
                    final boolean messageQueueIsEmpty = receivedMessageQueue
                            .isEmpty();
                    final boolean noRequestsToSubmit = outstandingRequests
                            .isEmpty() && !scheduler.thereAreRequestsToSubmit();
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
                if (outstandingRequests.isEmpty() && scheduler.shouldStop()) {
                    stopped.set();
                }
            }
        } finally {
            transmitter.setShuttingDown();
            scheduler.shutdown();
            transmitter.setStopped();
            try {
                transmitter.join(Settings.TRANSMITTER_SHUTDOWN_TIMEOUT);
            } catch (final InterruptedException e) {
                // ignore.
            }
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

    @Override
    public Ibis getLocalIbis() {
        return localIbis;
    }
}

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

class MawEngine extends Thread implements MessageReceiveListener,
        RegistryEventHandler, EngineInterface {
    private static final IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.MEMBERSHIP_UNRELIABLE,
            IbisCapabilities.ELECTIONS_STRICT);
    private static final String MASTER_ELECTION_NAME = "master-election";
    private final ReceivedMessageQueue receivedMessageQueue = new ReceivedMessageQueue(
            Settings.MAXIMAL_RECEIVED_MESSAGE_QUEUE_LENGTH);
    private final TimeStatistics receivedMessageQueueStatistics = new TimeStatistics();
    /** If set, the master is still waiting for job submissions to handle. */
    private final Flag waitingForSubmissions = new Flag(true);
    private final Transmitter transmitter;
    private final ConcurrentLinkedQueue<IbisIdentifier> deletedNodes = new ConcurrentLinkedQueue<IbisIdentifier>();
    private final ConcurrentLinkedQueue<IbisIdentifier> newWorkers = new ConcurrentLinkedQueue<IbisIdentifier>();
    private final ConcurrentLinkedQueue<ExecuteJobMessage> workQueue = new ConcurrentLinkedQueue<ExecuteJobMessage>();
    private final PacketUpcallReceivePort receivePort;
    private final Ibis localIbis;
    private int activeWorkers = 0;
    private long receivedMessageHandlingTime = 0;
    private long requestsHandlingTime = 0;
    private long idleTime = 0;
    private final boolean isMaster;
    private final WorkerAdministration workerAdministration = new WorkerAdministration();

    private final Scheduler scheduler;

    MawEngine() throws IbisCreationFailedException, IOException {
        super("LearningMaster engine thread");
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
            // scheduler = new RoundRobinScheduler();
            scheduler = new LearningScheduler();
        } else {
            scheduler = new WorkerScheduler(masterIdentifier);
            // As a worker, we don't wait for submissions.
            waitingForSubmissions.set(false);
        }
        receivePort = new PacketUpcallReceivePort(localIbis,
                Globals.receivePortName, this);
        transmitter.start();
        registry.enableEvents();
        receivePort.enable();
        if (!isMaster) {
            // Tell the master we're ready.
            transmitter.addToBookkeepingQueue(masterIdentifier,
                    new RegisterWorkerMessage());
        }
        if (Settings.TraceNodeCreation) {
            Globals.log.reportProgress("Created ibis " + myIbis + " "
                    + Utils.getPlatformVersion() + " host "
                    + Utils.getCanonicalHostname());
            Globals.log.reportProgress(" isMaster=" + isMaster);
        }
    }

    @Override
    public void died(final IbisIdentifier worker) {
        if (worker.equals(localIbis.identifier())) {
            if (!isInterrupted()) {
                Globals.log
                        .reportError("This worker has been declared dead, we might as well stop");
                interrupt();
            }
        } else {
            transmitter.deleteNode(worker);
            deletedNodes.add(worker);
        }
        wakeEngineThread(); // Something interesting has happened.
    }

    @Override
    public void left(final IbisIdentifier worker) {
        if (worker.equals(localIbis.identifier())) {
            if (!isInterrupted()) {
                Globals.log
                        .reportError("This worker has been declared `left', we might as well stop");
                interrupt();
            }
        } else {
            transmitter.deleteNode(worker);
            deletedNodes.add(worker);
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
    public void joined(final IbisIdentifier worker) {
        newWorkers.add(worker);
        if (Settings.TraceEngine) {
            Globals.log.reportProgress("New worker " + worker);
        }
        wakeEngineThread(); // Something interesting has happened.
    }

    /**
     * Register the fact that no new workers can join this pool. Not relevant,
     * so we ignore this.
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
     * @param node
     *            The node that may be dead.
     */
    @Override
    public void setSuspect(final IbisIdentifier node) {
        if (Settings.TraceNodes) {
            Globals.log.reportProgress(true, "Node " + node + " may be dead");
        }
        try {
            localIbis.registry().assumeDead(node);
        } catch (final IOException e) {
            // Nothing we can do about it.
        }
    }

    private void registerNewNode(final IbisIdentifier node) {
        if (node.equals(localIbis.identifier())) {
            // That's the local node. Ignore.
            return;
        }
        activeWorkers++;
        if (Settings.TraceNodes) {
            Globals.log.reportProgress("Node " + node + " has joined");
        }
        /*
         * We only add it to the administration when it has sent a join message.
         * Otherwise we might send it work before it has initialized.
         */
    }

    private void registerNodeLeft(final IbisIdentifier node) {
        if (Settings.TraceNodes) {
            Globals.log.reportProgress("Node " + node + " has left");
        }
        activeWorkers--;
        workerAdministration.removeWorker(node, scheduler);
        scheduler.removeNode(node);
    }

    /**
     * Handle any new and deleted nodes that have been registered after the last
     * call.
     * 
     * @return <code>true</code> iff we made any changes to the node state.
     */
    private boolean registerNewAndDeletedNodes() {
        boolean changes = false;
        while (true) {
            final IbisIdentifier node = deletedNodes.poll();
            if (node == null) {
                break;
            }
            registerNodeLeft(node);
            changes = true;
        }
        while (true) {
            final IbisIdentifier node = newWorkers.poll();
            if (node == null) {
                break;
            }
            registerNewNode(node);
            changes = true;
        }
        return changes;
    }

    private void setStopped() {
        interrupt();
        wakeEngineThread(); // Something interesting has happened.
    }

    /**
     * Handles an incoming message.
     * 
     * @param message
     *            The message to handle.
     */
    @Override
    public void messageReceived(final Message message) {
        // We are not allowed to do I/O in this thread, and we shouldn't
        // take too much time, so put all messages in a local queue to be
        // handled by the main loop.
        try {
            receivedMessageQueue.add(message);
        } catch (final InterruptedException e) {
            // Ignore
        }
        if (Settings.TraceReceiver) {
            Globals.log.reportProgress("Added to receive queue: " + message);
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
        if (msg instanceof ExecuteJobMessage) {
            final ExecuteJobMessage r = (ExecuteJobMessage) msg;
            workQueue.add(r);
        } else if (msg instanceof JobCompletedMessage) {
            final JobCompletedMessage jobCompletedMessage = (JobCompletedMessage) msg;
            workerAdministration.removeJob(msg.source,
                    jobCompletedMessage.jobNo, jobCompletedMessage.failed);
        } else if (msg instanceof RegisterWorkerMessage) {
            final RegisterWorkerMessage registerWorkerMessage = (RegisterWorkerMessage) msg;
            final IbisIdentifier worker = registerWorkerMessage.source;
            workerAdministration.addWorker(worker);
            scheduler.workerHasJoined(worker);
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
            Globals.log.reportProgress("Get incoming message from queue: "
                    + msg);
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
     * If the work queue is not empty, get a job request from the work queue and
     * handle it.
     * 
     * @return <code>true</code> iff we actually handled a job request.
     */
    private boolean handleAWorkRequest() {
        final ExecuteJobMessage request = workQueue.poll();

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
        boolean failed = false;
        try {
            if (job instanceof AtomicJob) {
                final AtomicJob aj = (AtomicJob) job;
                res = aj.run(request.input);
            } else {
                Globals.log
                        .reportInternalError("Don't know how to execute a job of type "
                                + job.getClass());
                return false;
            }
        } catch (final JobFailedException x) {
            Globals.log.reportError("Execution of job " + job + " failed", x);
            failed = true;
            res = null;
        }
        final long endTime = System.nanoTime();
        if (Settings.TraceWorker) {
            Globals.log.reportProgress("Ended execution of job " + job);
        }
        final Message msg = new JobCompletedMessage(request.id, res, failed,
                1e-9 * (endTime - startTime));
        transmitter.addToBookkeepingQueue(request.source, msg);
        return true;
    }

    /**
     * On the master, make sure there are enough outstanding requests.
     */
    private boolean maintainOutstandingRequests() {
        final long start = System.nanoTime();
        final boolean progress = scheduler.maintainOutstandingRequests(
                transmitter, workerAdministration);
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
        Globals.log
                .reportProgress("=== Main thread was only woken by timeout ===");
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
                + deletedNodes.size() + " deleted nodes");
        transmitter.dumpState();
        scheduler.dumpState();
        workerAdministration.dumpState();
        Globals.log.reportProgress("=== End of dump of engine state ===");
    }

    @Override
    public void run() {
        final int sleepTime = Settings.MAXIMAL_ENGINE_SLEEP_INTERVAL;
        try {
            while (true) {
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
                                workerAdministration.dumpState();
                            }
                        }
                    }
                } while (progress);
                if (!waitingForSubmissions.isSet()
                        && workerAdministration.isEmpty()
                        && scheduler.shouldStop()) {
                    Globals.log
                            .reportProgress("Setting engine to stopped state");
                    interrupt();
                    break;
                }
                synchronized (this) {
                    final boolean messageQueueIsEmpty = receivedMessageQueue
                            .isEmpty();
                    final boolean noRequestsToSubmit = workerAdministration
                            .isEmpty() && !scheduler.thereAreRequestsToSubmit();
                    if (noRequestsToSubmit && messageQueueIsEmpty
                            && newWorkers.isEmpty() && deletedNodes.isEmpty()
                            && workQueue.isEmpty()) {
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
                            // We've been interrupted, stop.
                            break;
                        }
                    }
                }
                if (sleptLong) {
                    if (activeWorkers > 0) {
                        dumpEngineState();
                    }
                }
            }
        } finally {
            transmitter.setShuttingDown();
            scheduler.shutdown();
            transmitter.setStopped();
            try {
                transmitter.join(Settings.TRANSMITTER_SHUTDOWN_TIMEOUT);
            } catch (final InterruptedException e) {
                // Somebody wants us to stop.
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

    public boolean isMaster() {
        return isMaster;
    }

    public void submitRequest(final AtomicJob job, final Serializable input) {
        scheduler.submitRequest(job, input);
    }

    public void endRequests() {
        Globals.log
                .reportProgress("All requests have been submitted; waiting for work queue to drain");
        waitingForSubmissions.set(false);
        wakeEngineThread();
    }
}

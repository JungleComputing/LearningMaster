package ibis.learningmaster;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.Registry;
import ibis.ipl.RegistryEventHandler;
import ibis.steel.Estimator;
import ibis.steel.ExponentialDecayEstimator;
import ibis.steel.ExponentialDecayLogEstimator;
import ibis.steel.GaussianEstimator;
import ibis.steel.LogGaussianEstimator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

class StochasticLearningPing extends Thread implements PacketReceiveListener,
        EngineInterface, RegistryEventHandler {
    private static final int PINGCOUNT = 1000000;
    private final Transmitter transmitter;
    private final ConcurrentLinkedQueue<IbisIdentifier> deletedPeers = new ConcurrentLinkedQueue<IbisIdentifier>();
    private final ConcurrentLinkedQueue<IbisIdentifier> newPeers = new ConcurrentLinkedQueue<IbisIdentifier>();
    private final Flag stopped = new Flag(false);
    private final PacketUpcallReceivePort receivePort;
    private final Ibis localIbis;
    private final ReceivedMessageQueue receivedMessageQueue = new ReceivedMessageQueue(
            Settings.MAXIMAL_RECEIVED_MESSAGE_QUEUE_LENGTH);
    private long receivedMessageHandlingTime = 0;
    private long idleTime = 0;
    private final PingAdministration pingAdministration = new PingAdministration();
    private static final IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.MEMBERSHIP_UNRELIABLE,
            IbisCapabilities.ELECTIONS_UNRELIABLE);

    private static class PingMessage extends SmallMessage {
        private static final long serialVersionUID = 1L;

        PingMessage() {
            // Nothing
        }
    }

    private static class PingReplyMessage extends SmallMessage {
        private static final long serialVersionUID = 1L;

        PingReplyMessage() {
            // Nothing
        }

    }

    private static class NodeAdministration {
        final IbisIdentifier id;
        private int pingsToSend;
        private int pingsToReceive;
        private final Estimator estimators[] = {
                new LogGaussianEstimator(1e-3, 1e-1),
                new GaussianEstimator(1e-3, 1e-3),
                new ExponentialDecayLogEstimator(1e-3, 1e-1),
                new ExponentialDecayEstimator(1e-3, 1e-3) };
        private long latestPingSentTime;

        NodeAdministration(final IbisIdentifier id) {
            this.id = id;
            pingsToSend = PINGCOUNT;
            pingsToReceive = PINGCOUNT;
        }

        void registerSentPing() {
            latestPingSentTime = System.nanoTime();
        }

        void registerReceivedPing() {
            pingsToReceive--;
        }

        boolean needsContact() {
            return pingsToSend > 0 || pingsToReceive > 0;
        }

        boolean registerReceivedPingReply(final long arrivalTime) {
            boolean sendAnotherPing = false;
            final long t = arrivalTime - latestPingSentTime;
            final double v = t == 0 ? 1e-11 : 1e-9 * t;
            for (final Estimator e : estimators) {
                e.addSample(v);
            }
            if (pingsToSend > 0) {
                sendAnotherPing = true;
                pingsToSend--;
            }
            return sendAnotherPing;
        }

        void printStatistics(final PrintStream printStream) {
            printStream.println(id.toString() + ":");
            for (final Estimator e : estimators) {
                printStream.println("  " + e.getName() + ": "
                        + e.getStatisticsString());
            }
        }
    }

    private static class PingAdministration {
        private final ArrayList<NodeAdministration> nodes = new ArrayList<NodeAdministration>();
        private boolean sawAnyPeers = false;

        PingAdministration() {
            // Nothing
        }

        private static int findNode(final ArrayList<NodeAdministration> l,
                final IbisIdentifier id) {
            for (int i = 0; i < l.size(); i++) {
                final NodeAdministration n = l.get(i);
                if (id.equals(n.id)) {
                    return i;
                }
            }
            return -1;
        }

        void registerNewPeer(final IbisIdentifier peer) {
            sawAnyPeers = true;
            nodes.add(new NodeAdministration(peer));
        }

        void registerSentPing(final IbisIdentifier id) {
            final int ix = findNode(nodes, id);
            if (ix < 0) {
                Globals.log.reportInternalError("Sent ping to unknown node "
                        + id);
            } else {
                final NodeAdministration n = nodes.get(ix);
                n.registerSentPing();
            }
        }

        void registerReceivedPing(final IbisIdentifier id) {
            final int ix = findNode(nodes, id);
            if (ix < 0) {
                Globals.log
                        .reportInternalError("Received ping from unknown node "
                                + id);
            } else {
                final NodeAdministration n = nodes.get(ix);
                n.registerReceivedPing();
            }
        }

        boolean registerReceivedPingReply(final IbisIdentifier id,
                final long receiveTime) {
            boolean sendAnotherPing = false;
            final int ix = findNode(nodes, id);
            if (ix < 0) {
                Globals.log
                        .reportInternalError("Received ping from unknown node "
                                + id);
            } else {
                final NodeAdministration n = nodes.get(ix);
                sendAnotherPing = n.registerReceivedPingReply(receiveTime);
            }
            return sendAnotherPing;
        }

        boolean shouldStop() {
            if (!sawAnyPeers) {
                return false;
            }
            for (final NodeAdministration nd : nodes) {
                if (nd.needsContact()) {
                    return false;
                }
            }
            return true;
        }

        void printStatistics(final PrintStream printStream) {
            for (final NodeAdministration n : nodes) {
                n.printStatistics(printStream);
            }
        }
    }

    public StochasticLearningPing() throws IbisCreationFailedException,
            IOException {
        transmitter = new Transmitter(this);
        final Properties ibisProperties = new Properties();
        localIbis = IbisFactory.createIbis(ibisCapabilities, ibisProperties,
                true, this, PacketSendPort.portType,
                PacketUpcallReceivePort.portType);
        final Registry registry = localIbis.registry();
        final IbisIdentifier myIbis = localIbis.identifier();
        receivePort = new PacketUpcallReceivePort(localIbis,
                Globals.receivePortName, this);
        transmitter.start();
        registry.enableEvents();
        receivePort.enable();
        if (Settings.TraceNodeCreation) {
            Globals.log.reportProgress("Created ibis " + myIbis + " "
                    + Utils.getPlatformVersion() + " host "
                    + Utils.getCanonicalHostname());
        }
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
            changes = true;
        }
        while (true) {
            final IbisIdentifier peer = newPeers.poll();
            if (peer == null) {
                break;
            }
            pingAdministration.registerNewPeer(peer);
            sendPing(peer);
            changes = true;
        }
        return changes;
    }

    private boolean handleIncomingMessages() {
        final long start = System.nanoTime();
        boolean progress = false;
        while (true) {
            final Message msg = receivedMessageQueue.getNext();
            if (msg == null) {
                break;
            }
            handleMessage(msg);
            progress = true;
        }
        final long duration = System.nanoTime() - start;
        receivedMessageHandlingTime += duration;
        return progress;
    }

    private void handleMessage(final Message msg) {
        if (msg instanceof PingMessage) {
            final PingMessage m = (PingMessage) msg;
            pingAdministration.registerReceivedPing(m.source);
            sendPingReply(m.source);
        } else if (msg instanceof PingReplyMessage) {
            final PingReplyMessage m = (PingReplyMessage) msg;
            final boolean sendAnotherPing = pingAdministration
                    .registerReceivedPingReply(m.source, m.arrivalTime);
            if (sendAnotherPing) {
                sendPing(m.source);
            }
        } else {
            Globals.log.reportInternalError("Don't know how to handle a "
                    + msg.getClass() + " message");
        }

    }

    private void sendPing(final IbisIdentifier destination) {
        final PingMessage msg = new PingMessage();
        pingAdministration.registerSentPing(destination);
        transmitter.addToRequestQueue(destination, msg);
    }

    private void sendPingReply(final IbisIdentifier destination) {
        final PingReplyMessage reply = new PingReplyMessage();
        transmitter.addToRequestQueue(destination, reply);
    }

    private static void runExperiment() {
        StochasticLearningPing p;
        try {
            p = new StochasticLearningPing();
            p.start();
            p.join();
        } catch (final IbisCreationFailedException e) {
            System.err.println("Could not create ibis: "
                    + e.getLocalizedMessage());
            e.printStackTrace();
            System.err.println("Goodbye!");
            System.exit(2);
        } catch (final InterruptedException e) {
            System.err.println("Interrupted: " + e.getLocalizedMessage());
            e.printStackTrace();
            System.err.println("Goodbye!");
            System.exit(2);
        } catch (final IOException e) {
            System.err.println("I/O exception: " + e.getLocalizedMessage());
            e.printStackTrace();
            System.err.println("Goodbye!");
            System.exit(2);
        }
    }

    private static PrintStream openPrintFile(final String s) {
        try {
            return new PrintStream(new File(s));
        } catch (final FileNotFoundException e) {
            System.err.println("Cannot open file '" + s + "' for writing: "
                    + e.getLocalizedMessage());
            e.printStackTrace();
            System.exit(1);
            return null; // To satisfy the compiler.
        }
    }

    public static void main(final String args[]) {
        runExperiment();
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
                    progress = progressIncoming || progressPeerChurn;
                    if (Settings.TraceDetailedProgress) {
                        if (progress) {
                            Globals.log.reportProgress("EE p=true i="
                                    + progressIncoming + " c="
                                    + progressPeerChurn);
                            if (progressIncoming) {
                                receivedMessageQueue.printCounts();
                            }
                        }
                    }
                } while (progress);
                synchronized (this) {
                    final boolean messageQueueIsEmpty = receivedMessageQueue
                            .isEmpty();
                    if (!stopped.isSet() && messageQueueIsEmpty
                            && newPeers.isEmpty() && deletedPeers.isEmpty()) {
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
                    dumpEngineState();
                }
                if (pingAdministration.shouldStop()) {
                    stopped.set();
                }
            }
        } finally {
            transmitter.setShuttingDown();
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
        final PrintStream printStream = Globals.log.getPrintStream();
        printStatistics(printStream);
        pingAdministration.printStatistics(printStream);
        transmitter.printStatistics(printStream);
        Utils.printThreadStats(printStream);
    }

    private void printStatistics(final PrintStream printStream) {
    }

    private void dumpEngineState() {
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

    @Override
    public void died(final IbisIdentifier peer) {
        if (peer.equals(localIbis.identifier())) {
            if (!stopped.isSet()) {
                Globals.log
                        .reportError("We have been declared dead, we might as well stop");
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
        // Ignore
    }

    @Override
    public void gotSignal(final String arg0, final IbisIdentifier arg1) {
        // Ignore
    }

    @Override
    public void joined(final IbisIdentifier peer) {
        newPeers.add(peer);
        if (Settings.TraceEngine) {
            Globals.log.reportProgress("New peer " + peer);
        }
        wakeEngineThread(); // Something interesting has happened.
    }

    @Override
    public void left(final IbisIdentifier peer) {
        if (peer.equals(localIbis.identifier())) {
            if (!stopped.isSet()) {
                Globals.log
                        .reportError("They think we have left the pool, we might as well stop");
                setStopped();
            }
        } else {
            transmitter.deletePeer(peer);
            deletedPeers.add(peer);
        }
        wakeEngineThread(); // Something interesting has happened.
    }

    @Override
    public void poolClosed() {
        // Ignore
    }

    @Override
    public void poolTerminated(final IbisIdentifier arg0) {
        setStopped();
    }

    @Override
    public void wakeEngineThread() {
        synchronized (this) {
            notifyAll();
        }
    }

    @Override
    public void setSuspect(final IbisIdentifier destination) {
        // Ignore
    }

    @Override
    public Ibis getLocalIbis() {
        return localIbis;
    }
}

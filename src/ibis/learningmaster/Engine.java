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
    private static final String SPECIAL_PEER_ELECTION_NAME = "special-peer-election";
    private final ReceivedMessageQueue receivedMessageQueue = new ReceivedMessageQueue(
            Settings.MAXIMAL_RECEIVED_MESSAGE_QUEUE_LENGTH);
    private final Flag stopped = new Flag(false);
    private final Transmitter transmitter;
    private final ConcurrentLinkedQueue<IbisIdentifier> deletedPeers = new ConcurrentLinkedQueue<IbisIdentifier>();
    private final ConcurrentLinkedQueue<IbisIdentifier> newPeers = new ConcurrentLinkedQueue<IbisIdentifier>();
    private final PacketUpcallReceivePort receivePort;
    final Ibis localIbis;
    private int activePeers = 0;
    private long receivedMessageHandlingTime = 0;
    private long requestsHandlingTime = 0;
    private long idleTime = 0;
    private long sendQueueHandlingTime;
    private final boolean isSpecialPeer;
    
    private OutstandingRequestList outstandingRequests = new OutstandingRequestList();
    
    private Scheduler scheduler = new Scheduler();

    Engine(
            final boolean helper)
            throws IbisCreationFailedException, IOException {
        super("Arnold engine thread");
        boolean runForSpecialNode = true;
        if (helper) {
            runForSpecialNode = false;
        }
        this.transmitter = new Transmitter(this);
        final Properties ibisProperties = new Properties();
        this.localIbis = IbisFactory.createIbis(ibisCapabilities,
                ibisProperties, true, this, PacketSendPort.portType,
                PacketUpcallReceivePort.portType);
        final Registry registry = localIbis.registry();
        final IbisIdentifier myIbis = localIbis.identifier();
        if (runForSpecialNode) {
            final IbisIdentifier m = registry.elect(SPECIAL_PEER_ELECTION_NAME);
            isSpecialPeer = m.equals(myIbis);
        } else {
            isSpecialPeer = false;
        }
			receivePort = new PacketUpcallReceivePort(localIbis,
			        Globals.receivePortName, this);
        System.out.println("runForSpecialNode=" + runForSpecialNode);
        transmitter.start();
        registry.enableEvents();
        receivePort.enable();
        if (Settings.TraceNodeCreation) {
            Globals.log.reportProgress("Created ibis " + myIbis + " "
                    + Utils.getPlatformVersion() + " host "
                    + Utils.getCanonicalHostname());
            Globals.log
                    .reportProgress(
                             " specialPeer=" + isSpecialPeer);
            Utils.showSystemProperties(Globals.log.getPrintStream(), "arnold.");
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
        activePeers++;
        if (Settings.TracePeers) {
            Globals.log.reportProgress("Peer " + peer + " has joined");
        }
        scheduler.addPeer(peer);
    }

    private void registerPeerLeft(final IbisIdentifier peer) {
        if (Settings.TracePeers) {
            Globals.log.reportProgress("Peer " + peer + " has left");
        }
        activePeers--;
        outstandingRequests.removePeer(peer, workers);
        scheduler.removePeer(peer);
    }

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

    @Override
    public void cancelPieceDownload(final IbisIdentifier peer, final int piece) {
        if (Settings.TracePieceTraffic) {
            final PrintStream s = Globals.log.getPrintStream();
            s.println("CANCELEDDOWNLOAD\t" + System.currentTimeMillis() + "\t"
                    + piece + "\t" + peer + "\t" + localIbis.identifier());
        }
        outstandingRequests.cancelPieceDownload(peer, piece, transmitter);
    }

    @Override
    public void startPieceDownload(final IbisIdentifier peer, final int piece) {
        if (Settings.TracePieceTraffic) {
            final PrintStream s = Globals.log.getPrintStream();
            s.println("STARTDOWNLOAD\t" + System.currentTimeMillis() + "\t"
                    + piece + "\t" + peer + "\t" + localIbis.identifier());
        }
        outstandingRequests.add(peer, piece, sharedFile.getPieceSize(piece));
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
            scheduler.addChunkRequest(r.source, r.chunk);
            scheduler.updateCredit(r.source, r.credit);
        } else if (msg instanceof PieceMessage) {
            handlePieceMessage((PieceMessage) msg);
        } else if (msg instanceof RequestPiecesMessage) {
            handleRequestPiecesMessage((RequestPiecesMessage) msg);
        } else if (msg instanceof InterestedMessage) {
            handleInterestedMessage((InterestedMessage) msg);
        } else if (msg instanceof BitSetMessage) {
            handleBitSetMessage((BitSetMessage) msg);
        } else if (msg instanceof CancelMessage) {
            final CancelMessage c = (CancelMessage) msg;
            scheduler.removeChunkRequest(c.source, c.chunk);
        } else if (msg instanceof HaveMessage) {
            handleHaveMessage((HaveMessage) msg);
        } else if (msg instanceof JoinHelpersMessage) {
            handleJoinHelpersMessage((JoinHelpersMessage) msg);
        } else if (msg instanceof ResignAsHelperMessage) {
            handleResignAsHelperMessage((ResignAsHelperMessage) msg);
        } else if (msg instanceof CloseConnectionMessage) {
            handleCloseConnectionMessage((CloseConnectionMessage) msg);
        } else {
            Globals.log.reportInternalError("Don't know how to handle a "
                    + msg.getClass() + " message");
        }
    }

    private void handleInterestedMessage(final InterestedMessage m) {
        scheduler.setPeerIsInterested(m.source, m.flag);
    }


    private void handleJoinHelpersMessage(final JoinHelpersMessage m) {
        scheduler.peerHasJoinedAsProxyHelper(m.source, personality);
    }

    private void handleResignAsHelperMessage(final ResignAsHelperMessage m) {
        scheduler.peerHasResignedAsProxyHelper(m.source);
    }

    private void handleCloseConnectionMessage(final CloseConnectionMessage m) {
        scheduler.handleClosedConnection(m.source);
    }

    private void handleRequestPiecesMessage(final RequestPiecesMessage msg) {
        scheduler.requestPieces(msg.bits);
    }

    private void handlePieceMessage(final PieceMessage msg) {
        final int piece = msg.piece;
        if (sharedFile.isValidPiece(piece)) {
            // Somebody sent us data for a piece we already have.
            // Ignore it.
            return;
        }
        scheduler.updateCredit(msg.source, msg.credit);
        scheduler.registerReceivedChunk(msg.source, msg.data.length);
        final byte completedPiece[] = outstandingRequests.updatePiece(
                msg.source, piece, msg.offset, msg.data);
        if (completedPiece != null) {
            // That completed our piece.
            boolean valid;
            try {
                valid = sharedFile.storePiece(piece, completedPiece);
            } catch (final IOException e) {
                Globals.log.reportError("Failed to write piece " + piece + ": "
                        + e.getLocalizedMessage());
                e.printStackTrace();
                throw new DownloadFailedError("Failed to write piece " + piece,
                        e);
            }
            if (valid) {
                outstandingRequests.cancelPieceDownload(piece, transmitter);
                // Deduct the bytes we got from this piece from our account.
                scheduler.registerCompletedPiece(msg.source, piece);
                if (Settings.TracePieceTraffic) {
                    final PrintStream s = Globals.log.getPrintStream();
                    s.println("COMPLETEDDOWNLOAD\t"
                            + System.currentTimeMillis() + "\t" + piece + "\t"
                            + msg.source + "\t" + localIbis.identifier());
                }
            } else {
                scheduler.registerIncorrectPiece(msg.source, piece);
                if (Settings.TracePieceTraffic) {
                    final PrintStream s = Globals.log.getPrintStream();
                    s.println("FAILEDDOWNLOAD\t" + System.currentTimeMillis()
                            + "\t" + piece + "\t" + msg.source + "\t"
                            + localIbis.identifier());
                }
            }
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
     * Fulfill the request for one chunk.
     * 
     * @return <code>true</code> iff we actually fulfilled a chunk request.
     */
    private boolean fulfilChunkRequest() {
        final ChunkRequest request = scheduler.getNextChunkRequest();

        if (request == null) {
            // No requests.
            return false;
        }
        final Chunk chunk = request.chunk;
        final byte data[];
        try {
            data = sharedFile.readChunk(chunk);
        } catch (final IOException e) {
            Globals.log.reportError("Cannot read chunk " + chunk
                    + " from shared file");
            e.printStackTrace();
            return false;
        }
        final Message piece = new PieceMessage(credit.getValue(), chunk.piece,
                chunk.offset, data);
        transmitter.addToDataQueue(request.peer, piece);
        return true;
    }

    private boolean thereAreRequestsToFulfill() {
        return transmitter.needsMoreData()
                && scheduler.haveIncomingChunkRequests();
    }

    private boolean keepSendQueueFilled() {
        final long start = System.nanoTime();
        boolean progress = false;
        if (transmitter.needsMoreData()) {
            progress = fulfilChunkRequest();
            if (!progress) {
                progress = scheduler.generateMoreTransmission();
            }
        }
        final long duration = System.nanoTime() - start;
        sendQueueHandlingTime += duration;
        return progress;
    }

    /**
     * Make sure there are enough outstanding requests.
     */
    private boolean maintainOutstandingRequests() {
        final long start = System.nanoTime();
        final boolean progress = outstandingRequests.maintainRequests(
                transmitter);
        final long duration = System.nanoTime() - start;
        requestsHandlingTime += duration;
        return progress;
    }

    private synchronized void printStatistics(final PrintStream s) {
        s.println("message handling "
                + Utils.formatSeconds(1e-9 * receivedMessageHandlingTime)
                + " requests handling "
                + Utils.formatSeconds(1e-9 * requestsHandlingTime)
                + " send queue handling "
                + Utils.formatSeconds(1e-9 * sendQueueHandlingTime) + " idle "
                + Utils.formatSeconds(1e-3 * idleTime));
        receivedMessageQueueStatistics.printStatistics(s,
                "receive queue linger time");
    }

    private synchronized void dumpEngineState() {
        Globals.log.reportProgress("Main thread was only woken by timeout");
        receivedMessageQueue.dump();
        receivedMessageQueueStatistics.printStatistics(Globals.log
                .getPrintStream(), "receive queue linger time");
        Globals.log.reportProgress("Maximal receive queue length: "
                + receivedMessageQueue.getMaximalQueueLength());
        Globals.log.reportProgress("message handling "
                + Utils.formatSeconds(1e-9 * receivedMessageHandlingTime)
                + " requests handling "
                + Utils.formatSeconds(1e-9 * requestsHandlingTime)
                + " send queue handling "
                + Utils.formatSeconds(1e-9 * sendQueueHandlingTime) + " idle "
                + Utils.formatSeconds(1e-3 * idleTime));
        Globals.log.reportProgress("Engine: " + activePeers + " active, "
                + deletedPeers.size() + " deleted peers");
        outstandingRequests.dumpState();
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
                    final boolean progressIncoming = handleIncomingMessages();
                    final boolean progressPeerChurn = registerNewAndDeletedPeers();
                    final boolean progressRequests = maintainOutstandingRequests();
                    final boolean progressSendQueue = keepSendQueueFilled();
                    progress = progressIncoming | progressPeerChurn
                            | progressRequests | progressSendQueue;
                    if (Settings.TraceDetailedProgress) {
                        // if (sharedFile.isComplete()) {
                        if (progress) {
                            Globals.log.reportProgress("EE p=true i="
                                    + progressIncoming + " c="
                                    + progressPeerChurn + " r="
                                    + progressRequests + " s="
                                    + progressSendQueue);
                            if (progressIncoming) {
                                receivedMessageQueue.printCounts();
                            }
                            if (progressRequests) {
                                outstandingRequests.dumpState();
                            }
                        }
                    }
                } while (progress);
                synchronized (this) {
                    final boolean messageQueueIsEmpty = receivedMessageQueue
                            .isEmpty();
                    final boolean noRequestsToFulfill = !thereAreRequestsToFulfill();
                    final boolean noRequestsToSubmit = !outstandingRequests
                            .requestsToSubmit();
                    if (!stopped.isSet() && messageQueueIsEmpty
                            && newPeers.isEmpty() && deletedPeers.isEmpty()
                            && noRequestsToFulfill && noRequestsToSubmit) {
                        try {
                            final long sleepStartTime = System
                                    .currentTimeMillis();
                            if (Settings.TraceEngine) {
                                Globals.log
                                        .reportProgress("Main loop: waiting");
                            }
                            this.wait(sleepTime);
                            final long sleepInterval = System
                                    .currentTimeMillis()
                                    - sleepStartTime;
                            idleTime += sleepInterval;
                            sleptLong = sleepInterval > 9 * sleepTime / 10;
                        } catch (final InterruptedException e) {
                            // Ignored.
                        }
                    }
                }
                if (sleptLong) {
                    if (activePeers > 0) {
                        dumpEngineState();
                    }
                }
                if (personality.shouldStop()) {
                    if (Settings.TracePersonalityActions) {
                        Globals.log.reportProgress("Personality "
                                + personality.getName()
                                + " says we should stop");
                        personality.dumpState();
                    }
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

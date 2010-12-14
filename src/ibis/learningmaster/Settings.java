package ibis.learningmaster;

class Settings {

    /** Message transmission timeout in ms on essential communications. */
    static final int COMMUNICATION_TIMEOUT = 2000;

    /**
     * The maximal time in ms we wait for the transmitter to shut down.
     */
    static final long TRANSMITTER_SHUTDOWN_TIMEOUT = 30000;

    protected static final int MAXIMAL_SEND_RETRIES = 5;

    /** Maximal number of messages in the receive queue before it blocks. */
    static final int MAXIMAL_RECEIVED_MESSAGE_QUEUE_LENGTH = 50;

    /** The ideal length of the data queue length of the transmitter. */
    static final int IDEAL_TRANSMITTER_QUEUE_LENGTH = 3;

    /** Do we cache connections? */
    static final boolean CACHE_CONNECTIONS = true;

    /** The number of connections we maximally keep open. */
    static final int CONNECTION_CACHE_SIZE = 50;

    /** How many cache accesses unused before the entry is evicted. */
    static final int CONNECTION_CACHE_MAXIMAL_UNUSED_COUNT = 2000;

    static final boolean TraceNodeCreation = true;
    static final boolean TraceEngine = false;
    static final boolean TracePeers = false;
    static final boolean TraceDetailedProgress = false;
    static final boolean TraceTransmitter = false;
    static final boolean TraceReceiver = false;
    static final boolean TraceSends = false;
    static final boolean TraceTransmitterLoop = false;
    static final boolean TraceWorker = false;

    static final int MAXIMAL_ENGINE_SLEEP_INTERVAL = 2000;

    static final int TASK_COUNT = 200;

    // 5 seconds
    static final int TASK_DURATION = 1000 * 5;
}

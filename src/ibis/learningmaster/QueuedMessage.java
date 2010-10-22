package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

/**
 * A entry in a message queue, containing a message and a destination.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class QueuedMessage {
    final IbisIdentifier destination;

    final Message msg;

    final long enqueueTime;

    int retries = 0;

    /**
     * @param destination
     * @param msg
     */
    QueuedMessage(final IbisIdentifier destination, final Message msg) {
        super();
        this.destination = destination;
        this.msg = msg;
        this.enqueueTime = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "QueuedMessage[to=" + destination + ",msg=" + msg + "]";
    }
}

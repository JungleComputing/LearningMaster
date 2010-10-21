package ibis.learningmaster;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A queue of (non-trivial) incoming messages.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class ReceivedMessageQueue {
    private final BlockingQueue<Message> q;
    private int maximalQueueLength = 0;
    private final HashMap<String, Integer> counts = new HashMap<String, Integer>();

    ReceivedMessageQueue(final int capacity) {
        this.q = new ArrayBlockingQueue<Message>(capacity, true);
    }

    /**
     * Returns the next message in the queue, or <code>null</code> if the queue
     * is empty.
     * 
     * @return The message.
     */
    Message getNext() {
        return q.poll();
    }

    private void countMessage(final Message m) {
        final Class<? extends Message> cl = m.getClass();
        final String nm = cl.getSimpleName();
        synchronized (counts) {
            if (counts.containsKey(nm)) {
                final Integer n = counts.get(nm);
                counts.put(nm, n + 1);
            } else {
                counts.put(nm, 1);
            }
        }
    }

    void printCounts() {
        synchronized (counts) {
            final Set<String> keys = counts.keySet();
            System.out.print("Received messages:");
            for (final String k : keys) {
                final Integer n = counts.get(k);
                System.out.print(" " + k + ":" + n);
            }
        }
        System.out.println();
    }

    void add(final Message msg) {
        try {
            q.put(msg);
            final int sz = q.size();
            if (maximalQueueLength < sz) {
                maximalQueueLength = sz;
            }
            countMessage(msg);
        } catch (final InterruptedException e) {
            Globals.log
                    .reportInternalError("Got interrupt waiting for receive message queue  to drain");
        }
    }

    boolean isEmpty() {
        return q.isEmpty();
    }

    void dump() {
        Globals.log.reportProgress("There are " + q.size()
                + " messages in the receive queue");
    }

    int getMaximalQueueLength() {
        return maximalQueueLength;
    }

}

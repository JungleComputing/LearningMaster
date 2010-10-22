/**
 * 
 */
package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Kees van Reeuwijk
 * 
 */
class SendQueue {

    private final ConcurrentLinkedQueue<QueuedMessage> q = new ConcurrentLinkedQueue<QueuedMessage>();
    private final TimeStatistics queingStatistics = new TimeStatistics();
    private int maximalQueueLength = 0;

    void add(final IbisIdentifier destination, final Message msg) {
        q.add(new QueuedMessage(destination, msg));
        synchronized (this) {
            final int sz = q.size();
            if (maximalQueueLength < sz) {
                maximalQueueLength = sz;
            }
        }
    }

    void add(final QueuedMessage msg) {
        q.add(msg);
    }

    QueuedMessage getNext() {
        final QueuedMessage msg = q.poll();
        if (msg != null) {
            final long waittime = System.currentTimeMillis() - msg.enqueueTime;
            queingStatistics.registerSample(waittime * 1e-3);
        }
        return msg;
    }

    boolean isEmpty() {
        return q.isEmpty();
    }

    int size() {
        return q.size();
    }

    synchronized void printStatistics(final PrintStream s, final String name) {
        queingStatistics.printStatistics(s, name + " linger time");
        Globals.log.reportProgress("  current length of " + name + ": "
                + q.size() + " maximal: " + maximalQueueLength);
    }

    void clear() {
        q.clear();
    }
}

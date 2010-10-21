package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * A port that communicates in entire objects.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class PacketSendPort {
    static final PortType portType = new PortType(
            PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT,
            PortType.CONNECTION_MANY_TO_ONE, PortType.RECEIVE_AUTO_UPCALLS,
            PortType.RECEIVE_EXPLICIT);

    private final ConnectionCache connectionCache;

    private long sentBytes = 0;

    private double sendTime = 0;

    private int sentCount = 0;

    /**
     * The list of known destinations. Register a destination before trying to
     * send to it.
     */
    private final HashMap<IbisIdentifier, DestinationInfo> destinations = new HashMap<IbisIdentifier, DestinationInfo>();

    /** One entry in the list of destinations. */
    private static final class DestinationInfo {
        private static final class InfoComparator implements
                Comparator<DestinationInfo>, Serializable {
            private static final long serialVersionUID = 9141273343902181193L;

            /**
             * Compares the two given destination info class instances. This
             * comparator ensures that the class instances are sorted by
             * decreasing sentCount. To provide a stable sort when the sentCount
             * is the same (can happen for corner cases), it also compares for
             * other fields.
             * 
             * @param a
             *            The first class instance to compare.
             * @param b
             *            The second class instance to compare.
             * @return The comparison result.
             */
            @SuppressWarnings("synthetic-access")
            @Override
            public int compare(final DestinationInfo a, final DestinationInfo b) {
                if (a.sentCount < b.sentCount) {
                    return 1;
                }
                if (a.sentCount > b.sentCount) {
                    return -1;
                }
                if (a.sentBytes < b.sentBytes) {
                    return 1;
                }
                if (a.sentBytes > b.sentBytes) {
                    return -1;
                }
                return 0;
            }

        }

        private int sentCount = 0;

        private long sentBytes = 0;

        private final IbisIdentifier ibisIdentifier;

        /**
         * Create a new destination info entry.
         * 
         * @param ibisIdentifier
         *            The destination ibis.
         */
        private DestinationInfo(final IbisIdentifier ibisIdentifier) {
            this.ibisIdentifier = ibisIdentifier;
        }

        /** Print statistics for this destination. */
        private synchronized void printStatistics(final PrintStream s) {
            s.format(" %5d messages %6s   node %s\n", sentCount, Utils
                    .formatByteCount(sentBytes), ibisIdentifier.toString());
        }

        private synchronized void incrementSentCount() {
            sentCount++;
        }

        private synchronized void addSentBytes(final long val) {
            sentBytes += val;
        }
    }

    PacketSendPort(final Engine node) {
        connectionCache = new ConnectionCache(node);
    }

    /**
     * Given a receive port, registers it with this packet send port, and
     * returns an identifier of the port.
     * 
     * @param theIbis
     *            The port to register.
     */
    @SuppressWarnings("synthetic-access")
    private synchronized DestinationInfo registerDestination(
            final IbisIdentifier theIbis) {
        DestinationInfo destinationInfo = destinations.get(theIbis);
        if (destinationInfo != null) {
            // Already registered.
            return destinationInfo;
        }
        destinationInfo = new DestinationInfo(theIbis);
        destinations.put(theIbis, destinationInfo);
        return destinationInfo;
    }

    /**
     * Sends the given data to the given ibis.
     * 
     * @param theIbis
     *            The ibis to send it to.
     * @param message
     *            The data to send.
     * @return <code>true</code> if we managed to send the data.
     */
    @SuppressWarnings("synthetic-access")
    boolean sendMessage(final IbisIdentifier theIbis, final Message message) {
        long len;
        boolean ok = true;
        final DestinationInfo info = registerDestination(theIbis);
        info.incrementSentCount();
        {
            double t;

            final double startTime = Utils.getPreciseTime();
            len = connectionCache.sendMessage(theIbis, message);
            if (len < 0) {
                ok = false;
                len = 0;
            }
            synchronized (this) {
                sentBytes += len;
                sentCount++;
                t = Utils.getPreciseTime() - startTime;
                sendTime += t;
            }
            info.addSentBytes(len);
            if (Settings.TraceSends) {
                Globals.log.reportProgress("Sent " + len + " bytes in "
                        + Utils.formatSeconds(t) + ": " + message);
            }
        }
        return ok;
    }

    /**
     * Given the name of this port, prints some statistics about this port.
     * 
     * @param portname
     *            The name of the port.
     */
    @SuppressWarnings("synthetic-access")
    synchronized void printStatistics(final PrintStream s, final String portname) {
        s.println(portname + ": sent " + Utils.formatByteCount(sentBytes)
                + " in " + sentCount + " large messages");
        if (sentCount > 0) {
            s.println(portname + ": total send time  "
                    + Utils.formatSeconds(sendTime) + "; "
                    + Utils.formatSeconds(sendTime / sentCount)
                    + " per message");
        }
        final DestinationInfo l[] = new DestinationInfo[destinations.size()];
        int sz = 0;
        for (final Map.Entry<IbisIdentifier, DestinationInfo> entry : destinations
                .entrySet()) {
            final DestinationInfo i = entry.getValue();

            if (i != null) {
                l[sz++] = i;
            }
        }
        connectionCache.printStatistics(s);
        final Comparator<? super DestinationInfo> comparator = new DestinationInfo.InfoComparator();
        Arrays.sort(l, 0, sz, comparator);
        for (int ix = 0; ix < sz; ix++) {
            final DestinationInfo i = l[ix];

            i.printStatistics(s);
        }
    }
}

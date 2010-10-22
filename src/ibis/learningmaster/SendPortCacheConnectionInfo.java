package ibis.learningmaster;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;

import java.io.IOException;

final class SendPortCacheConnectionInfo {
    private SendPort port;

    private int mostRecentUse = 0;

    synchronized SendPort getPort(final Ibis localIbis,
            final IbisIdentifier remoteIbis, final int useCount) {
        if (port == null) {
            try {
                port = localIbis.createSendPort(PacketSendPort.portType);
                port.connect(remoteIbis, Globals.receivePortName,
                        Settings.COMMUNICATION_TIMEOUT, true);
            } catch (final IOException x) {
                try {
                    if (port != null) {
                        port.close();
                        port = null;
                    }
                } catch (final Throwable e) {
                    // Nothing we can do.
                }
            }
        }
        mostRecentUse = useCount;
        return port;
    }

    synchronized void close() {
        if (port != null) {
            try {
                port.close();
            } catch (final IOException e) {
                // Nothing we can do.
            }
            port = null;
        }
    }

    int getMostRecentUse() {
        return mostRecentUse;
    }
}
package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Maintains a cache of open connections.
 * 
 * @author Kees van Reeuwijk.
 */
class ConnectionCache {
	private final EngineInterface node;

	private final SendPortCache cache;

	ConnectionCache(final EngineInterface node2) {
		this.node = node2;
		this.cache = new SendPortCache(node2, Settings.CONNECTION_CACHE_SIZE,
				Settings.CONNECTION_CACHE_MAXIMAL_UNUSED_COUNT);
	}

	private long cachedSendMessage(final IbisIdentifier ibis,
			final Message message) {
		long len = -1;
		try {
			final SendPort port = cache.getSendPort(ibis);
			if (port == null) {
				// We could not create a connection to this ibis.
				// Presumably the node is dead.
				Globals.log.reportProgress("Could not get send port for ibis "
						+ ibis);
				cache.closeSendPort(ibis);
				return -1;
			}
			final WriteMessage msg = port.newMessage();
			msg.writeObject(message);
			len = msg.finish();
		} catch (final IOException x) {
			Globals.log.reportProgress("Could not get send port for ibis "
					+ ibis + ": " + x.getLocalizedMessage());
			final PrintStream printStream = Globals.log.getPrintStream();
			printStream.print("------- Original stack trace: --------");
			x.printStackTrace(printStream);
			node.setSuspect(ibis);
			cache.closeSendPort(ibis);
		}
		return len;
	}

	/**
	 * Given an ibis, returns a WriteMessage to use.
	 * 
	 * @param ibis
	 *            The ibis to send to.
	 * @return The WriteMessage to fill.
	 */
	private long uncachedSendMessage(final IbisIdentifier ibis,
			final Message message) {
		long len = -1;
		SendPort port = null;
		try {
			port = node.getLocalIbis().createSendPort(PacketSendPort.portType);
			port.connect(ibis, Globals.receivePortName,
					Settings.COMMUNICATION_TIMEOUT, true);
			WriteMessage msg = null;
			try {
				msg = port.newMessage();
				msg.writeObject(message);
			} finally {
				if (msg != null) {
					len = msg.finish();
				}
			}
			port.close();
			return len;
		} catch (final IOException x) {
			node.setSuspect(ibis);
		} finally {
			try {
				if (port != null) {
					port.close();
				}
			} catch (final Throwable x) {
				// Nothing we can do.
			}
		}
		return len;
	}

	/**
	 * Given an ibis and a message, send the message.
	 * 
	 * @param ibis
	 *            The ibis to send to.
	 * @param message
	 *            The message to send.
	 * @return The number of bytes that were transmitted.
	 */
	long sendMessage(final IbisIdentifier ibis, final Message message) {
		long sz;

		if (Settings.CACHE_CONNECTIONS) {
			sz = cachedSendMessage(ibis, message);
		} else {
			sz = uncachedSendMessage(ibis, message);
		}
		return sz;
	}

	void printStatistics(final PrintStream s) {
		cache.printStatistics(s);
	}
}

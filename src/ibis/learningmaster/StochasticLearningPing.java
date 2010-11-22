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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Properties;

class StochasticLearningPing extends Thread implements EngineInterface,
		RegistryEventHandler {
	private static int PINGCOUNT = 30000;
	private final Transmitter transmitter;
	private final Flag stopped = new Flag(false);
	private final Ibis localIbis;
	private final PeerAdministration peerAdministration = new PeerAdministration();
	private static final IbisCapabilities ibisCapabilities = new IbisCapabilities(
			IbisCapabilities.MEMBERSHIP_UNRELIABLE,
			IbisCapabilities.ELECTIONS_STRICT);

	public StochasticLearningPing() throws IbisCreationFailedException {
		this.transmitter = new Transmitter(this);
		final Properties ibisProperties = new Properties();
		this.localIbis = IbisFactory.createIbis(ibisCapabilities,
				ibisProperties, true, this, PacketSendPort.portType,
				PacketUpcallReceivePort.portType);
		final Registry registry = localIbis.registry();
		final IbisIdentifier myIbis = localIbis.identifier();
	}

	private static final class PeerAdministration {
		private final ArrayList<PeerInfo> peers = new ArrayList<PeerInfo>();

		void add(final IbisIdentifier peer) {
			peers.add(new PeerInfo(peer));
		}

		public void deletePeer(final IbisIdentifier peer) {
			for (int i = 0; i < peers.size(); i++) {
				final PeerInfo p = peers.get(i);
				if (peer.equals(p.peer)) {
					peers.remove(i);
					break;
				}
			}
		}

	}

	private static class PeerInfo {
		private final Estimator performance = buildEstimator();
		private final IbisIdentifier peer;
		private int pingsToSend = PINGCOUNT;
		private int pingsToReceive = PINGCOUNT;
		private final long latestPingSent = -1;
		private Estimator latestEstimate;

		static Estimator buildEstimator() {
			return new ExponentialDecayEstimator();
			// return new GaussianEstimator();
		}

		PeerInfo(final IbisIdentifier peer) {
			// Start with a very optimistic estimate to avoid corner cases
			this.peer = peer;
			performance.addSample(0);
		}

		public void addSample(final double v) {
			performance.addSample(v);
		}

		public void printStatistics(final PrintStream s, final String lbl) {
			performance.printStatistics(s, performance.getName() + ": " + lbl);
		}

		static String getName() {
			return buildEstimator().getName();
		}

		void sendAPing(final Transmitter t) {
			latestEstimate = performance.getEstimate();

			pingsToSend--;
		}

		void registerReceivedPing(final Message msg,
				final Transmitter transmitter) {
			final long t = msg.arrivalTime - latestPingSent;
			System.out.println("node: " + peer + " estimate="
					+ Utils.formatSeconds(t));
		}

		void registerRemotePing() {
			pingsToReceive--;
		}
	}

	private static void runExperiment(final PrintStream stream,
			final String lbl, final boolean verbose, final boolean printEndStats) {
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
		runExperiment(System.out, "test", false, true);
	}

	@Override
	public void run() {
	}

	private void setStopped() {
		stopped.set();
		wakeEngineThread(); // Something interesting has happened.
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
			peerAdministration.deletePeer(peer);
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
		peerAdministration.add(peer);
		if (Settings.TraceEngine) {
			Globals.log.reportProgress("New peer " + peer);
		}
		wakeEngineThread(); // Something interesting has happened.
		// TODO Auto-generated method stub

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
			peerAdministration.deletePeer(peer);
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
		// TODO Auto-generated method stub

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

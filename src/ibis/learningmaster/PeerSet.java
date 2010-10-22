package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.util.HashSet;

class PeerSet {
	private final HashSet<IbisIdentifier> set = new HashSet<IbisIdentifier>();

	synchronized void add(final IbisIdentifier peer) {
		set.add(peer);
	}

	synchronized boolean contains(final IbisIdentifier peer) {
		return set.contains(peer);
	}

	@Override
	public String toString() {
		return set.toString();
	}

}

package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.util.HashSet;

class NodeSet {
	private final HashSet<IbisIdentifier> set = new HashSet<IbisIdentifier>();

	synchronized void add(final IbisIdentifier node) {
		set.add(node);
	}

	synchronized boolean contains(final IbisIdentifier node) {
		return set.contains(node);
	}

	@Override
	public String toString() {
		return set.toString();
	}

}

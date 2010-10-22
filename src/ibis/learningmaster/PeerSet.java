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

    boolean remove(final IbisIdentifier peer) {
        return set.remove(peer);
    }

    boolean isEmpty() {
        return set.isEmpty();
    }

    IbisIdentifier extractRandomElement() {
        if (set.isEmpty()) {
            return null;
        }
        final Object[] l = set.toArray();
        final int ix = Globals.rng.nextInt(l.length);
        final Object res = l[ix];
        set.remove(res);
        return (IbisIdentifier) res;
    }

    @Override
    public String toString() {
        return set.toString();
    }

}

package ibis.learningmaster;

/**
 * A boolean with synchronous access.
 * 
 * @author Kees van Reeuwijk.
 */
class Flag {
    private boolean flag;

    Flag(final boolean flag) {
        this.flag = flag;
    }

    void set() {
        set(true);
    }

    synchronized void set(final boolean val) {
        final boolean changed = flag != val;
        flag = val;
        if (changed) {
            this.notifyAll();
        }
    }

    synchronized boolean isSet() {
        return flag;
    }

}

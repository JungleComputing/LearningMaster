package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

import java.io.Serializable;

abstract class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    protected volatile IbisIdentifier source;

    volatile long arrivalTime;

    @Override
    public String toString() {
        return Utils.toStringClassScalars(this);
    }
}

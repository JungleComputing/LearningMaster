package ibis.learningmaster;

import ibis.ipl.IbisIdentifier;

interface EngineInterface {

    void wakeEngineThread();

    void setSuspect(IbisIdentifier destination);
}

package ibis.learningmaster;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;

interface EngineInterface {

	void wakeEngineThread();

	void setSuspect(IbisIdentifier destination);

	Ibis getLocalIbis();
}

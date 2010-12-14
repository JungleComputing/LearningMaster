package ibis.learningmaster;

import java.io.Serializable;

/**
 * A message from a worker to a master, telling it it has completed a job.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class JobCompletedMessage extends Message {
    private static final long serialVersionUID = 1L;

    final int jobNo;
    final Serializable result;
    final boolean failed;
    final double completionTime;

    JobCompletedMessage(final int jobNo, final Serializable res,
            final boolean failed, final double completionTime) {
        this.jobNo = jobNo;
        this.result = res;
        this.failed = failed;
        this.completionTime = completionTime;
    }

    @Override
    public String toString() {
        return "JobCompletedMessage[" + jobNo + "]";
    }
}

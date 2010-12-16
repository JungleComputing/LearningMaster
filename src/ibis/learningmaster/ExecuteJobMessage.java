package ibis.learningmaster;

import java.io.Serializable;

/**
 * A message from a master to a worker, asking it to execute the given job with
 * the given input. An identifier is also added; it is used by the master for
 * its administration of outstanding jobs.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class ExecuteJobMessage extends SmallMessage {
    private static final long serialVersionUID = 1L;

    final Job job;

    final int id;

    final Serializable input;

    ExecuteJobMessage(final Job job, final int id, final Serializable input) {
        this.job = job;
        this.id = id;
        this.input = input;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof ExecuteJobMessage)) {
            return false;
        }
        final ExecuteJobMessage other = (ExecuteJobMessage) obj;
        return source.equals(other.source) && job.equals(other.job);
    }

    @Override
    public String toString() {
        return "RequestMessage[" + job + "]";
    }

    @Override
    public int hashCode() {
        return job.hashCode();
    }
}

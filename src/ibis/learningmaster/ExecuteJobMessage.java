package ibis.learningmaster;

import java.io.Serializable;

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

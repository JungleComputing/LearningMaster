package ibis.learningmaster;

import java.io.Serializable;

class ExecuteTaskMessage extends SmallMessage {
    private static final long serialVersionUID = 1L;

    final Job job;

    final int id;

    final Serializable input;

    ExecuteTaskMessage(final Job job, final int id, final Serializable input) {
        this.job = job;
        this.id = id;
        this.input = input;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof ExecuteTaskMessage)) {
            return false;
        }
        final ExecuteTaskMessage other = (ExecuteTaskMessage) obj;
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

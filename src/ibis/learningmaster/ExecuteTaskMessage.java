package ibis.learningmaster;

class ExecuteTaskMessage extends SmallMessage {
    private static final long serialVersionUID = 1L;

    final Job job;

    final int id;

    ExecuteTaskMessage(final Job job, final int id) {
        this.job = job;
        this.id = id;
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

package ibis.learningmaster;

class ExecuteTaskMessage extends SmallMessage {
    private static final long serialVersionUID = 1L;

    final int jobNo;

    ExecuteTaskMessage(int jobNo) {
        this.jobNo = jobNo;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof ExecuteTaskMessage)) {
            return false;
        }
        final ExecuteTaskMessage other = (ExecuteTaskMessage) obj;
        return this.source.equals(other.source) && this.jobNo == other.jobNo;
    }

    @Override
    public String toString() {
        return "RequestMessage[" + jobNo + "]";
    }

    @Override
    public int hashCode() {
        return jobNo;
    }
}

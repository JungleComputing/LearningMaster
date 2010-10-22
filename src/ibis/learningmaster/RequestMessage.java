package ibis.learningmaster;

class RequestMessage extends SmallMessage {
    private static final long serialVersionUID = 1L;

    final int jobNo;

    RequestMessage(int jobNo) {
        this.jobNo = jobNo;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof RequestMessage)) {
            return false;
        }
        final RequestMessage other = (RequestMessage) obj;
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

package ibis.learningmaster;

/**
 * A message from a worker to a master, telling it it has completed task 'task'.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class TaskCompletedMessage extends Message {
    private static final long serialVersionUID = 1L;

    public final int task;

    TaskCompletedMessage(int task) {
        this.task = task;
    }

    @Override
    public String toString() {
        return "PieceMessage[" + task + "]";
    }
}

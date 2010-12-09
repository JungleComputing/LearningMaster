package ibis.learningmaster;

import java.io.Serializable;

/**
 * A message from a worker to a master, telling it it has completed task 'task'.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class TaskCompletedMessage extends Message {
    private static final long serialVersionUID = 1L;

    final int task;
    Serializable result;
    double completionTime;

    TaskCompletedMessage(final int task, final Serializable res,
            final double completionTime) {
        this.task = task;
        this.result = res;
        this.completionTime = completionTime;
    }

    @Override
    public String toString() {
        return "TaskCompletedMessage[" + task + "]";
    }
}

package ibis.learningmaster;

import java.io.Serializable;

public class JobInstance {
    final Job job;
    final Serializable input;

    public JobInstance(final Job job, final Serializable input) {
        super();
        this.job = job;
        this.input = input;
    }

}

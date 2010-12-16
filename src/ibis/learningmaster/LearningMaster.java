package ibis.learningmaster;

import ibis.ipl.IbisCreationFailedException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;

/**
 * Main class of a learning master dispatcher.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class LearningMaster {

    private static void usage(final String msg, final String args[]) {
        System.err.println("Error: " + msg);
        System.err.println("Usage: LearningMaster <option>] ... <option>");
        System.err.println("Where <option> is:");
        System.err.println(" --worker\tNode is a worker");
        System.err.println("Actual arguments: " + Arrays.deepToString(args));
        throw new Error("Bad command-line arguments");
    }

    private static class SleepJob implements AtomicJob, Serializable {
        private final long sleepTime;
        private static final long serialVersionUID = 1L;

        class SleepJobType extends JobType {
            final long sleepTime;

            SleepJobType(long sleepTime) {
                this.sleepTime = sleepTime;
            }

            @Override
            public boolean equals(Object oth) {
                if (!(oth instanceof SleepJobType)) {
                    return false;
                }
                final SleepJobType other = (SleepJobType) oth;
                return this.sleepTime == other.sleepTime;
            }
        }

        SleepJob(long sleepTime) {
            this.sleepTime = sleepTime;
        }

        @Override
        public boolean isSupported() {
            return true;
        }

        @Override
        public Serializable run(final Serializable input)
                throws JobFailedException {
            try {
                Thread.sleep(sleepTime);
            } catch (final InterruptedException e) {
                // Ignore
            }
            return null;
        }

        @Override
        public JobType getJobType() {
            // TODO Auto-generated method stub
            return new SleepJobType(sleepTime);
        }
    }

    private static void setPropertyOption(final String arg) {
        final int eq = arg.indexOf('=');
        final Properties p = System.getProperties();
        if (eq < 0) {
            p.setProperty(arg.substring(2), "");
        } else {
            p.setProperty(arg.substring(2, eq), arg.substring(eq + 1));
        }
    }

    /**
     * @param args
     *            The command-line arguments.
     */
    public static void main(final String[] args) {
        // First parse the command line.
        for (final String arg : args) {
            if (arg.startsWith("-D")) {
                setPropertyOption(arg);
            } else if (arg.startsWith("--")) {
                usage("Unknown option '" + arg + '\'', args);
            }
        }
        try {
            final MawEngine e = new MawEngine();
            if (e.isMaster()) {
                for (int i = 0; i < Settings.JOB_COUNT; i++) {
                    e.submitRequest(new SleepJob(300), null);
                }
                e.endRequests();
            }
            e.start();
            e.join();
        } catch (final IbisCreationFailedException e) {
            System.err.println("Could not create ibis: "
                    + e.getLocalizedMessage());
            e.printStackTrace();
            System.err.println("Goodbye!");
            System.exit(2);
        } catch (final InterruptedException e) {
            System.err.println("Main thread got interrupt");
            e.printStackTrace();
        } catch (final IOException e) {
            System.err.println("I/O error");
            e.printStackTrace();
        }
    }
}

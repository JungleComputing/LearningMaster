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
        System.err
                .println(" --dummyfile\tShare a dummy file instead of a real one");
        System.err
                .println(" --proxymode\tRun proxy mode with the leecher hiding behind helpers");
        System.err.println(" --helper\tPeer is a helper in a proxy-mode setup");
        System.err.println("Actual arguments: " + Arrays.deepToString(args));
        throw new Error("Bad command-line arguments");
    }

    private static class SleepJob implements AtomicJob, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isSupported() {
            return true;
        }

        @Override
        public Serializable run(final Serializable input)
                throws JobFailedException {
            final Long time = (Long) input;
            try {
                Thread.sleep(time);
            } catch (final InterruptedException e) {
                // Ignore
            }
            return null;
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
                for (int i = 0; i < Settings.TASK_COUNT; i++) {
                    e.submitRequest(new SleepJob());
                }
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

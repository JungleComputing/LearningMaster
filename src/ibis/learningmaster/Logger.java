package ibis.learningmaster;

import java.io.PrintStream;

/**
 * Handle logging events.
 * 
 * @author Kees van Reeuwijk
 */
class Logger {
    private final PrintStream logfile;
    private final long startTime = System.currentTimeMillis();

    /** Create a new logger. */
    Logger() {
        logfile = System.out;
    }

    /**
     * Report to the user that some progress has been made.
     * 
     * @param msg
     *            The message to send to the user.
     */
    void reportProgress(final String msg) {
        reportProgress(false, msg);
    }

    private void printTimeStamp() {
        final long interval = System.currentTimeMillis() - startTime;
        logfile.format("%5.3f ", 1e-3 * interval);
    }

    /**
     * Report to the user that some progress has been made.
     * 
     * @param stacktrace
     *            If set, also print a stack trace.
     * @param msg
     *            The message to send to the user.
     */
    void reportProgress(final boolean stacktrace, final String msg) {
        printTimeStamp();
        logfile.println(msg);
        logfile.flush();
        if (stacktrace) {
            final Throwable t = new Throwable();
            t.printStackTrace(logfile);
        }
    }

    /**
     * Given an error message, report an error.
     * 
     * @param msg
     *            The error message.
     */
    void reportError(final String msg) {
        printTimeStamp();
        logfile.print("Error: ");
        logfile.println(msg);
    }

    void reportError(final String msg, final JobFailedException x) {
        printTimeStamp();
        logfile.print("Error: ");
        logfile.println(msg + ':');
        x.printStackTrace(logfile);
    }

    /**
     * Given an error message, report an internal error.
     * 
     * @param msg
     *            The error message.
     */
    void reportInternalError(final String msg) {
        printTimeStamp();
        logfile.print("Internal error: ");
        logfile.println(msg);
        final Throwable t = new Throwable();
        t.printStackTrace(logfile);
    }

    /**
     * Returns the print stream of this logger.
     * 
     * @return The print stream.
     */
    PrintStream getPrintStream() {
        return logfile;
    }
}

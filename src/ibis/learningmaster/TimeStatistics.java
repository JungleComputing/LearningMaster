package ibis.learningmaster;

import java.io.PrintStream;

/**
 * Maintain time statistics.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class TimeStatistics {
    private double sum = 0;
    private double maxValue = Double.MIN_VALUE;
    private double minValue = Double.MAX_VALUE;
    private int samples = 0;

    void registerSample(final double d) {
        samples++;
        sum += d;
        if (d > maxValue) {
            maxValue = d;
        }
        if (d < minValue) {
            minValue = d;
        }
    }

    void printStatistics(final PrintStream s, final String label) {
        if (samples == 0) {
            s.println(label + ": no samples");
        } else {
            s.println(label + ": samples=" + samples + " average="
                    + Utils.formatSeconds(sum / samples) + " minimum="
                    + Utils.formatSeconds(minValue) + " maximum="
                    + Utils.formatSeconds(maxValue) + " total="
                    + Utils.formatSeconds(sum));

        }
    }
}

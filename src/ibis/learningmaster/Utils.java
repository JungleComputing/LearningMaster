package ibis.learningmaster;

import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.Properties;

class Utils {
    private static final double NANOSECOND = 1e-9;

    private static final double MICROSECOND = 1e-6;

    private static final double MILLISECOND = 1e-3;

    private static final double SECOND = 1.0;

    /**
     * Returns a string with the platform version that is used.
     * 
     * @return The platform version.
     */
    protected static String getPlatformVersion() {
        final java.util.Properties p = System.getProperties();

        return "Java " + p.getProperty("java.version") + " ("
                + p.getProperty("java.vendor") + ") on "
                + p.getProperty("os.name") + ' ' + p.getProperty("os.version")
                + " (" + p.getProperty("os.arch") + ')';
    }

    /**
     * Given a time in seconds, return a neat format string for it.
     * 
     * @param t
     *            The time to format.
     * @return The formatted string.
     */
    protected static String formatSeconds(final double t) {
        if (t == Double.POSITIVE_INFINITY) {
            return "infinite";
        }
        if (t == 0.0) {
            return "0 s";
        }
        if (t < MICROSECOND && t > -MICROSECOND) {
            return String.format("%4.1f ns", 1e9 * t).trim();
        }
        if (t < MILLISECOND && t > -MILLISECOND) {
            return String.format("%4.1f us", 1e6 * t).trim();
        }
        if (t < SECOND && t > -SECOND) {
            return String.format("%4.1f ms", 1e3 * t).trim();
        }
        return String.format("%4.1f s", t).trim();
    }

    /**
     * Divide <code>val</code> by <code>divisor</code>, rounding up to the next
     * integer.
     * 
     * @param val
     *            The nominator of the division.
     * @param divisor
     *            The denominator of the division.
     * @return The result of the division.
     */
    static int divideRoundUp(final long val, final long divisor) {
        return (int) ((val + divisor - 1) / divisor);
    }

    /**
     * Given a byte count, return a human-readable representation of it.
     * 
     * @param n
     *            The byte count to represent.
     * @return The byte count as a human-readable string.
     */
    protected static String formatByteCount(final long n) {
        if (n < 1000) {
            // This deliberately covers negative numbers
            return n + "B";
        }
        if (n < 1000000) {
            return String.format("%.1fKB", n * 1e-3);
        }
        if (n < 1000000000L) {
            return String.format("%.1fMB", n * 1e-6);
        }
        if (n < 1000000000000L) {
            return String.format("%.1fGB", n * 1e-9);
        }
        return String.format("%.1fTB", n * 1e-12);
    }

    static void printThreadStats(final PrintStream s) {
        final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

        s.println("Peak thread count: " + threadBean.getPeakThreadCount());
        final long lockedThreads[] = threadBean.findDeadlockedThreads();
        if (lockedThreads != null && lockedThreads.length > 0) {
            s.println("===== DEADLOCKED threads =====");
            for (final long tid : lockedThreads) {
                final ThreadInfo ti = threadBean.getThreadInfo(tid,
                        Integer.MAX_VALUE);
                if (ti != null) {
                    s.println(ti.toString());
                }
            }
        }
    }

    /**
     * @return Return the precise current time in seconds.
     */
    protected static double getPreciseTime() {
        return NANOSECOND * System.nanoTime();
    }

    /**
     * Given an array of bytes, compute its SHA-1 digest.
     * 
     * @param data
     *            The data to compute the digest of.
     * @return The SHA-1 digest: a 20-byte (160-bit) array.
     * @throws NoSuchAlgorithmException
     */
    static byte[] computeSHA1(final byte data[], final int length)
            throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-1");
        digest.update(data, 0, length);
        final byte res[] = digest.digest();
        return res;
    }

    static byte[] computeSHA1(final byte data[])
            throws NoSuchAlgorithmException {
        return computeSHA1(data, data.length);
    }

    static String bytesToHexString(final byte[] input) {
        final StringBuffer buf = new StringBuffer();
        for (final byte b : input) {
            buf.append(String.format("%02X", b));
        }
        return buf.toString();
    }

    static String getCanonicalHostname() {
        String res;

        try {
            final InetAddress localMachine = InetAddress.getLocalHost();
            res = localMachine.getCanonicalHostName();
        } catch (final UnknownHostException e) {
            res = null;
        }
        return res;
    }

    /**
     * Returns true iff the given class represents a primitive type, where we
     * consider String to be such a primitive type.
     * 
     * @param c
     *            The class under consideration.
     * @return <code>true</code> iff the class represents a primitive type.
     */
    private static boolean isPrimitiveType(final Class<?> c) {
        return c == java.lang.Boolean.TYPE || c == java.lang.Character.TYPE
                || c == java.lang.Byte.TYPE || c == java.lang.Short.TYPE
                || c == java.lang.Integer.TYPE || c == java.lang.Long.TYPE
                || c == java.lang.Float.TYPE || c == java.lang.Double.TYPE
                || c.equals(String.class);
    }

    private static boolean writeFields(final StringBuffer buf,
            final Class<? extends Object> clazz, final Object o, boolean first) {
        final Class<? extends Object> superClazz = clazz.getSuperclass();
        if (superClazz != null) {
            first = writeFields(buf, superClazz, o, first);
        }
        final Field[] fields = clazz.getDeclaredFields();
        for (final Field f : fields) {
            final Class<?> t = f.getType();
            if (isPrimitiveType(t)) {
                if (first) {
                    first = false;
                } else {
                    buf.append(' ');
                }
                final String nm = f.getName();
                buf.append(nm);
                buf.append('=');
                try {
                    f.setAccessible(true);
                    if (t.equals(String.class)) {
                        buf.append('"');
                        buf.append(f.get(o));
                        buf.append('"');
                    } else {
                        buf.append(f.get(o));
                    }
                } catch (final IllegalAccessException x) {
                    buf.append("<hidden>");
                }
            }
        }
        return first;
    }

    static String toStringClassScalars(final Object o) {
        final StringBuffer buf = new StringBuffer();

        final Class<? extends Object> clazz = o.getClass();
        buf.append(clazz.getSimpleName());
        buf.append('[');
        final boolean first = true;
        writeFields(buf, clazz, o, first);
        buf.append(']');
        return buf.toString();
    }

    static void showSystemProperties(final PrintStream s, final String prefix) {
        final Properties pl = System.getProperties();
        final Enumeration<Object> keys = pl.keys();
        while (keys.hasMoreElements()) {
            final String key = (String) keys.nextElement();
            if (key.startsWith(prefix)) {
                s.println(key + "=" + pl.getProperty(key));
            }
        }
    }

    static double getDoubleProperty(final String pnm, final double deflt) {
        final String b = System.getProperty(pnm);
        if (b == null) {
            return deflt;
        }
        return Double.parseDouble(b);
    }

    static boolean getExistenceProperty(final String pnm) {
        final String b = System.getProperty(pnm);
        return b != null;
    }

    static int getIntProperty(final String pnm, final int deflt) {
        final String b = System.getProperty(pnm);
        if (b == null) {
            return deflt;
        }
        return Integer.parseInt(b);
    }
}

package ibis.learningmaster;

import java.util.PriorityQueue;

class EventQueue {
    private final PriorityQueue<Event> q = new PriorityQueue<EventQueue.Event>();
    private final boolean verbose;
    private double lastRetrievedEventTime = -1;

    EventQueue(boolean verbose) {
        this.verbose = verbose;
    }

    static class Event implements Comparable<Event> {
        protected final double time;

        Event(double time) {
            this.time = time;
        }

        @Override
        public int compareTo(Event other) {
            int res;
            if (this.time > other.time) {
                res = 1;
            } else if (this.time < other.time) {
                res = -1;
            } else {
                res = 0;
            }
            return res;
        }

        double getTime() {
            return time;
        }
    }

    /**
     * Adds a new event to this queue. Events are returned with the earliest
     * event first.
     * 
     * @param e
     *            The event to add.
     */
    void addEvent(Event e) {
        if (e.time < lastRetrievedEventTime) {
            System.out.println("INTERNAL ERROR: event " + e
                    + " has earlier time than last retreived time "
                    + lastRetrievedEventTime);
        }
        if (verbose) {
            System.out.println("Added to event queue: " + e);
        }
        q.add(e);

    }

    /**
     * Returns the earliest event in this event queue, or <code>null</code> if
     * there are no events.
     */
    Event getNextEvent() {
        final Event e = q.poll();
        if (verbose) {
            System.out.println("Get from event queue: " + e);
        }
        if (e.time < lastRetrievedEventTime) {
            System.out.println("INTERNAL ERROR: events out of order: event "
                    + e + " has earlier time than last retrieved time "
                    + lastRetrievedEventTime);
        }
        lastRetrievedEventTime = e.time;
        return e;
    }
}

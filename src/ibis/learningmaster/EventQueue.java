package ibis.learningmaster;

import java.util.PriorityQueue;

class EventQueue {
    PriorityQueue<Event> q = new PriorityQueue<EventQueue.Event>();

    static class Event implements Comparable<Event> {
        protected final double time;

        Event(double time) {
            this.time = time;
        }

        @Override
        public int compareTo(Event other) {
            if (this.time > other.time) {
                return 1;
            }
            if (this.time < other.time) {
                return -1;
            }
            return 0;
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
        q.add(e);
    }

    /**
     * Returns the earliest event in this event queue, or <code>null</code> if
     * there are no events.
     */
    Event getNextEvent() {
        return q.poll();
    }
}

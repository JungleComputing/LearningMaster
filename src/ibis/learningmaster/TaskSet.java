package ibis.learningmaster;

import java.util.BitSet;

class TaskSet {
	private final BitSet set = new BitSet();
	private int smallest = 0;

	TaskSet(final int tasks) {
		set.set(0, tasks);
	}

	void add(final int task) {
		set.set(task);
		if (task < smallest) {
			smallest = task;
		}
	}

	boolean isEmpty() {
		return set.isEmpty();
	}

	void dumpState() {
		Globals.log.reportProgress("task set: " + toString());
	}

	int getNextTask() {
		if (set.isEmpty()) {
			Globals.log
					.reportInternalError("There is no next task to get; set is empty");
			return -1;
		}
		final int res = smallest;
		set.clear(res);
		smallest = set.nextSetBit(res + 1);
		return res;
	}

	@Override
	public String toString() {
		final StringBuffer buf = new StringBuffer();
		buf.append('{');
		int prev = -1;
		boolean first = true;
		boolean haveDash = false;
		for (int i = set.nextSetBit(0); i >= 0; i = set.nextSetBit(i + 1)) {
			if (first) {
				first = false;
				buf.append(i);
			} else {
				if (prev + 1 == i) {
					if (!haveDash) {
						buf.append('-');
						haveDash = true;
					}
				} else {
					if (haveDash) {
						buf.append(prev);
						haveDash = false;
					}
					buf.append(',');
					buf.append(i);
				}
			}
			prev = i;
		}
		if (haveDash) {
			buf.append(prev);
		}
		buf.append('}');
		return buf.toString();
	}
}

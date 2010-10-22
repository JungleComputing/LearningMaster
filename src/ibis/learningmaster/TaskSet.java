package ibis.learningmaster;

import java.util.BitSet;

class TaskSet {
	private final BitSet set = new BitSet();
	private int smallest = 0;

	TaskSet(final int tasks){
		set.set(0, tasks);
	}

	void add(final int task){
		set.set(task);
		if(task<smallest){
			smallest = task;
		}
	}

	boolean isEmpty(){
		return set.isEmpty();
	}

	void dumpState() {
		Globals.log.reportProgress("task set: " + set);
	}

	int getNextTask() {
		if(set.isEmpty()){
			Globals.log.reportInternalError("There is no next task to get; set is empty");
			return -1;
		}
		final int res = smallest;
		set.clear(res);
		smallest = set.nextSetBit(res+1);
		return res;
	}
}

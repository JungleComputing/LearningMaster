package ibis.learningmaster;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

class EstimatingLearner {
	private static final int WORKERS = 20;

	private static class Worker {
		private final double average;
		private final double likelyError;

		Worker(final double average, final double likelyError) {
			super();
			this.average = average;
			this.likelyError = likelyError;
		}

		double getValue() {
			return average + likelyError * Globals.rng.nextGaussian();
		}
	}

	private static class WorkerEstimator {
		private final Estimator performance = new Estimator();

		WorkerEstimator() {
			// Start with a very optimistic estimate to avoid corner cases
			performance.addSample(0);
		}

		public double getLikelyValue() {
			return performance.getLikelyValue();
		}

		public void addSample(final double v) {
			performance.addSample(v);
		}

		public void printStatistics(final String lbl) {
			performance.printStatistics(lbl);
		}
	}

	private static final Worker workers[] = new Worker[WORKERS];
	private static final WorkerEstimator workerEstimators[] = new WorkerEstimator[WORKERS];

	private static double submitAJob(final boolean verbose, final Worker[] wl,
			final WorkerEstimator[] wel) {
		double bestOffer = Double.MAX_VALUE;
		int bestWorker = -1;
		for (int i = 0; i < wl.length; i++) {
			final WorkerEstimator w = wel[i];
			final double offer = w.getLikelyValue();
			if (bestOffer > offer) {
				bestOffer = offer;
				bestWorker = i;
			}
		}
		final double resultValue = wl[bestWorker].getValue();
		if (verbose) {
			System.out.println("Worker " + bestWorker + " -> " + resultValue);
		}
		wel[bestWorker].addSample(resultValue);
		return resultValue;
	}

	private static void runExperiment(final PrintStream stream,
			final boolean verbose, final boolean printEndStats,
			final double fast, final double normal, final double slow,
			final double stddev, final int jobCount) {
		for (int i = 0; i < WORKERS; i++) {
			double v = normal;
			if (i == 1) {
				v = fast;
			} else if (i == 2) {
				v = slow;
			}
			workers[i] = new Worker(v, stddev * v);
			workerEstimators[i] = new WorkerEstimator();
		}
		double cost = 0;
		for (int i = 0; i < jobCount; i++) {
			cost += submitAJob(verbose, workers, workerEstimators);
		}
		if (printEndStats) {
			for (int i = 0; i < workerEstimators.length; i++) {
				workerEstimators[i].printStatistics(Integer.toString(i));
			}
			System.out.format(
					"Fast: %3g normal: %3g slow: %3g  Average cost: %.3g\n",
					fast, normal, slow, cost / jobCount);
		} else {
			stream.println(normal / fast + " " + cost / jobCount);
		}
	}

	private static PrintStream openPrintFile(final String s) {
		try {
			return new PrintStream(new File(s));
		} catch (final FileNotFoundException e) {
			System.err.println("Cannot open file '" + s + "' for writing: "
					+ e.getLocalizedMessage());
			e.printStackTrace();
			System.exit(1);
			return null; // To satisfy the compiler.
		}
	}

	// - sweep advantage of best worker from 0.1 to 0.9
	private static void generatePoints(final double slowFactor) {
		final double slowdowns[] = { 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50,
				100, 200 };
		final int SAMPLES = 10;
		final double fast = 100;

		for (final double s : slowdowns) {
			final double normal = fast * (1 + s);
			final double slow = slowFactor * normal;

			for (int sample = 0; sample < SAMPLES; sample++) {
				PrintStream stream;
				stream = openPrintFile("slow-machines.data");
				runExperiment(stream, false, true, fast, normal, slow, 0.01,
						30000);
				stream.close();
			}
		}
	}

	public static void main(final String args[]) {
		// runExperiment(true, 0.2, 100.0, 10000);
		generatePoints(1);
	}
}

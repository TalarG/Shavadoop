package shavadoop;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implements the Shavadoop master.
 * @author S.Cohard et T.Guzelbodur
 *
 */
public class Master {
	public static final boolean DEBUG = false;
	// whether local execution of
	// slaves is turned on (for
	// debugging)
	public static final int TASKS_PER_SLAVE = 1;


	/**
	 * Main entry point.
	 *
	 * @param args
	 *            the arguments, in order: the slave host file, the remote host
	 *            status file and the input file,
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	public static void main(final String[] args) throws IOException {
		if (args.length < 3) {
			throw new IllegalArgumentException(
					"Usage: <remote-hosts-file> <remote-host-status-file> <input-file> [<split-size>]");
		}
		new Master(args[0], args[1], args[2], args[3],
				args.length > 4 ? Integer.parseInt(args[4]) : 1);
	}

	private final List<String> reachableSlaves; // the list of reachable slave
												// hosts
	private Map<String, String> RMx_machines; // the mapping of RMx files to
												// slave hosts
	private Map<String, String> UMx_machines; // the mapping of UMx files to
												// slave hosts

	/**
	 * Creates a new instance of Master.
	 *
	 * @param slaveHostsFile
	 *            the pathname of the file containing the list of candidate
	 *            slave machines.
	 * @param slaveHostsStatusFile
	 *            the pathname of the file to write the slave machine status to.
	 * @param inputFile
	 *            the input file to process.
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	Master(final String slaveHostsFile, final String slaveHostsStatusFile,
			final String inputFile, final String outputFile, final int size)
			throws IOException {
		{
			System.out.println("Pinging slaves...");
			//Starting counting time
			long startTime = System.currentTimeMillis();
			final List<String> hosts = loadHostsFile(Paths.get(slaveHostsFile));
			reachableSlaves = pingSlaves(hosts, slaveHostsStatusFile);
			// end time
			long endTime = System.currentTimeMillis();
			//calculating total duration
			double totalTime = endTime - startTime;
			System.out.println("Pinging time: " + totalTime / 1000);
		}
		if (reachableSlaves.size() > 0) {
			List<String> Sx;
			{
				System.out.println("Splitting input file...");
				long startTime = System.currentTimeMillis();
				Sx = splitInputFile(inputFile, size);
				long endTime = System.currentTimeMillis();
				double totalTime = endTime - startTime;
				System.out.println(Sx.toString());
				System.out.println("Splitting time: " + totalTime / 1000);
			}
			Map<String, Set<String>> keys_UMx;
			{
				System.out.println("Mapping split files...");
				long startTime = System.currentTimeMillis();
				keys_UMx = mapSplitFiles(Sx, reachableSlaves);
				long endTime = System.currentTimeMillis();
				double totalTime = endTime - startTime;
				System.out.println("Mapping time: " + totalTime / 1000);
			}
			List<String> wordCounts;
			{
				System.out.println("Shuffle/reduce unsorted map files...");
				long startTime = System.currentTimeMillis();
				wordCounts = shuffleReduceMapFiles(keys_UMx, reachableSlaves);
				long endTime = System.currentTimeMillis();
				double totalTime = endTime - startTime;
				System.out.println("Shuffle/reduce time: " + totalTime / 1000);
			}
			{
				System.out.println("Assembling Final Result...");
				long startTime = System.currentTimeMillis();
				Collections.sort(wordCounts, new Comparator<String>() {
					@Override
					public int compare(final String o1, final String o2) {
						return Integer.valueOf(o2.split(":")[1])
								- Integer.valueOf(o1.split(":")[1]);
					}
				});
				long endTime = System.currentTimeMillis();
				double totalTime = endTime - startTime;
				System.out.println("Assembling time: " + totalTime / 1000);
			}
			System.out.println(wordCounts.subList(0,
					Math.min(50, wordCounts.size())));
			Files.write(Paths.get(outputFile), wordCounts,
					Charset.defaultCharset(), new OpenOption[0]);
		} else {
			System.err.println("No reachable slave hosts");
		}
	}

	/**
	 * Loads the list of candidate slave machines.
	 *
	 * @param file
	 *            the file pathname.
	 * @return the list of candidate slave machines.
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	private List<String> loadHostsFile(final Path file) throws IOException {
		return Files.readAllLines(file, Charset.defaultCharset());
	}

	/**
	 * Distributes the "map" job execution on the provided list of slave hosts.
	 *
	 * @param Sx
	 *            the list of split file pathnames.
	 * @param reachableSlaves
	 *            the list of reachable slave hosts.
	 * @return a map that associates to each of the slave hosts the result of
	 *         its computation.
	 */
	private Map<String, List<String>> mapRemoteExec(final List<String> Sx,
			final List<String> reachableSlaves) {
		final Map<String, List<String>> results = new HashMap<>();
		final List<SlaveThread> slaveThreads = new ArrayList<>();
		// Start threads for every single slave computation.
		final Iterator<String> Sx_iter = Sx.iterator();
		while (Sx_iter.hasNext()) {
			slaveThreads.clear();
			loop: for (int i = 0; i < TASKS_PER_SLAVE; i++) {
				for (final String slave : reachableSlaves) {
					if (!Sx_iter.hasNext()) {
						break loop;
					}
					final String Si = Sx_iter.next();
					final SlaveThread slaveThread = SlaveThread
							.createMapSlaveThread(slave, Si);
					slaveThreads.add(slaveThread);
					slaveThread.start();
				}
			}
			// Wait for all the slave threads to complete.
			waitForSlaveThreads(slaveThreads);
			// Consolidate results from slave threads
			for (final SlaveThread slaveThread : slaveThreads) {
				final String slave = slaveThread.getHost();
				if (!results.containsKey(slave)) {
					results.put(slave, new ArrayList<String>());
				}
				final List<String> output = slaveThread.getOutput();
				if (output != null) {
					results.get(slave).addAll(output);
				}
			}
		}
		// Returns the consolidated results of the slave computations.
		return results;
	}

	/**
	 * Implements the map stage.
	 *
	 * @param Sx
	 *            the list of split file pathnames.
	 * @param reachableSlaves
	 *            the list of reachable slave hosts.
	 * @return a mapping of keys (words) to the UMx files that contain these
	 *         words.
	 */
	private Map<String, Set<String>> mapSplitFiles(final List<String> Sx,
			final List<String> reachableSlaves) {
		final Map<String, List<String>> results = mapRemoteExec(Sx,
				reachableSlaves);
		UMx_machines = new HashMap<>();
		final Map<String, Set<String>> keys_UMx = new HashMap<>();
		if (DEBUG)
			System.err.println(results);
		for (final Map.Entry<String, List<String>> entry : results.entrySet()) {
			final String host = entry.getKey();
			for (final String pair : entry.getValue()) {
				final String[] items = pair.split(":");
				final String word = items[0];
				final String UMxFile = items[1];
				if (!keys_UMx.containsKey(word)) {
					keys_UMx.put(word, new HashSet<String>());
				}
				keys_UMx.get(word).add(UMxFile);
				UMx_machines.put(UMxFile, host);
			}
		}
		if (DEBUG)
			System.err.println("Keys_UMx:" + keys_UMx);
		if (DEBUG)
			System.err.println("UMx_machines:" + UMx_machines);
		return keys_UMx;
	}

	/**
	 * Pings each of the slave machines from the provided list.
	 *
	 * @param hosts
	 *            the list of candidate slave machines.
	 * @return a mapping of slave machine host names to their respective status.
	 */
	private Map<String, Boolean> pingRemoteExec(final List<String> hosts) {
		final List<SlaveThread> slaveThreads = new ArrayList<>();
		// Start threads for every single slave computation.
		for (final String host : hosts) {
			final SlaveThread slaveThread = SlaveThread
					.createPingSlaveThread(host);
			slaveThreads.add(slaveThread);
			slaveThread.start();
		}
		// Wait for all the slave threads to complete.
		waitForSlaveThreads(slaveThreads);
		// Consolidate results from slave threads
		final Map<String, Boolean> results = new HashMap<>();
		for (final SlaveThread slaveThread : slaveThreads) {
			if (DEBUG)
				System.err.println(slaveThread.getOutput());
			final List<String> output = slaveThread.getOutput();
			results.put(slaveThread.getHost(), output != null
					&& output.size() > 0 && "OK".equals(output.get(0)));
		}
		// Returns the consolidated results of the slave computations.
		return results;
	}

	/**
	 * Pings each of the slave machines from the provided list and save the
	 * result into the specified file.
	 *
	 * @param hosts
	 *            the list of candidate slave machines.
	 * @param slaveHostsStatusFile
	 *            the pathname of the file to write the slave machine status to.
	 * @return the list of reachable slave hosts.
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	private List<String> pingSlaves(final List<String> hosts,
			final String slaveHostsStatusFile) throws IOException {
		final Map<String, Boolean> results = pingRemoteExec(hosts);
		saveReachabilityStatus(Paths.get(slaveHostsStatusFile), results);
		final List<String> reachableHosts = new ArrayList<>();
		for (final String host : results.keySet()) {
			if (results.get(host)) {
				reachableHosts.add(host);
			}
		}
		return reachableHosts;
	}

	/**
	 * Reads the content of the specified input file.
	 *
	 * @param inputFile
	 *            the input file.
	 * @return the lines from the input file as a List.
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	private List<String> readInputFile(final Path inputFile) throws IOException {
		final List<String> lines = Files.readAllLines(inputFile,
				Charset.defaultCharset());
		return lines;
	}

	/**
	 * Saves the provided slave reachability results into the specified file.
	 *
	 * @param file
	 *            the file to save the results to.
	 * @param results
	 *            a mapping of slave hosts to reachability status.
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	private void saveReachabilityStatus(final Path file,
			final Map<String, Boolean> results) throws IOException {
		final List<CharSequence> lines = new ArrayList<>();
		for (final Map.Entry<String, Boolean> entry : results.entrySet()) {
			lines.add(entry.getKey() + ": " + entry.getValue());
		}
		Files.write(file, lines, Charset.defaultCharset(), new OpenOption[0]);
	}

	/**
	 * Implements the shuffle/reduce stage.
	 *
	 * @param keys_UMx
	 *            the mapping of keys (words) to the UMx files that contain
	 *            these words.
	 * @param reachableSlaves
	 *            the list of reachable slave hosts.
	 * @return
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	private List<String> shuffleReduceMapFiles(
			final Map<String, Set<String>> keys_UMx,
			final List<String> reachableSlaves) throws IOException {
		final Map<String, List<String>> results = shuffleReduceRemoteExec(
				keys_UMx, reachableSlaves);
		RMx_machines = new HashMap<>();
		final List<String> unsortedResults = new ArrayList<>();
		if (DEBUG)
			System.err.println(results);
		for (final List<String> wordCounts : results.values()) {
			unsortedResults.addAll(wordCounts);
		}

		return unsortedResults;
	}

	/**
	 * Distributes the "shuffle/reduce" job execution on the provided list of
	 * slave hosts.
	 *
	 * @param keys_UMx
	 *            a mapping of keys (words) to the UMx files that contain these
	 *            words.
	 * @param reachableSlaves
	 *            the list of reachable slave hosts.
	 * @return a map that associates to each word its number of occurrences.
	 */
	private Map<String, List<String>> shuffleReduceRemoteExec(
			final Map<String, Set<String>> keys_UMx,
			final List<String> reachableSlaves) {
		final Map<String, List<String>> results = new HashMap<>();
		final List<SlaveThread> slaveThreads = new ArrayList<>();
		// Start threads for every single slave computation.
		int i = 0;
		final Iterator<String> keys_iter = keys_UMx.keySet().iterator();
		while (keys_iter.hasNext()) {
			slaveThreads.clear();
			loop: for (int j = 0; j < TASKS_PER_SLAVE; j++) {
				for (final String slave : reachableSlaves) {
					if (!keys_iter.hasNext()) {
						break loop;
					}
					final String word = keys_iter.next();
					if (word.length() > 0) {
						final String RMi = "RM" + i++;
						final Set<String> UMx = keys_UMx.get(word);
						final SlaveThread slaveThread = SlaveThread
								.createReduceShuffleSlaveThread(slave, word,
										RMi, UMx);
						slaveThreads.add(slaveThread);
						slaveThread.start();
					}
				}
			}
			// Wait for all the slave threads to complete.
			waitForSlaveThreads(slaveThreads);
			// Consolidate results from slave threads
			for (final SlaveThread slaveThread : slaveThreads) {
				final String slave = slaveThread.getHost();
				if (!results.containsKey(slave)) {
					results.put(slave, new ArrayList<String>());
				}
				final List<String> output = slaveThread.getOutput();
				if (output != null) {
					results.get(slave).addAll(output);
				}
			}
		}
		// Returns the consolidated results of the slave computations.
		return results;
	}

	/**
	 * Splits the specified input file. Current implementation only splits on
	 * each line.
	 *
	 * @param inputFile
	 *            the input file to split.
	 * @return the path names of the files containing the splits.
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	private List<String> splitInputFile(final String inputFile, int size)
			throws IOException {
		final List<String> splitInputFiles = new ArrayList<>();
		final List<String> lines = readInputFile(Paths.get(inputFile));
		int count = 0;
		List<String> split = new ArrayList<String>();
		String line;
		for (int i = 0; i < lines.size(); i++) {
			line = lines.get(i);
			if (line.trim().length() > 0) {
				split.add(line);
			}
			if (split.size() >= size || (i + 1) >= lines.size()) {
				final String splitFile = "S" + count++;
				Files.write(Paths.get(splitFile), split,
						Charset.defaultCharset(), new OpenOption[0]);
				splitInputFiles.add(splitFile);
				split.clear();
			}
		}
		return splitInputFiles;
	}

	/**
	 * Waits for all designated slave threads.
	 *
	 * @param slaveThreads
	 *            the threads to wait for (join with).
	 */
	private void waitForSlaveThreads(final List<SlaveThread> slaveThreads) {
		for (final Thread slaveThread : slaveThreads) {
			while (true) {
				try {
					slaveThread.join();
					break;
				} catch (final InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

}

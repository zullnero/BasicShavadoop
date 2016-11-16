package shavadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This class handles the remote execution of a specific slave in a dedicated
 * thread.
 *
 * @author zull
 *
 */
class SlaveThread extends Thread {
	public static final boolean DEBUG = true; // whether local execution of
	                                          // slaves is turned on (for
	                                          // debugging)
	public static final String SLAVE_JAR = "/users/zull/BasicShavadoopSlave.jar"; // XXX:
	                                                                              // make
	                                                                              // configurable

	/**
	 * Creates a SlaveThread instance to manage the execution of the "MAP"
	 * command on the designated slave host.
	 *
	 * @param host
	 *            the slave host.
	 * @return the SlaveThread instance.
	 */
	static SlaveThread createMapSlaveThread(final String host, final String splitFile) {
		return new SlaveThread(host, "MAP", Arrays.asList(splitFile));
	}

	/**
	 * Creates a SlaveThread instance to manage the execution of the "PING"
	 * command on the designated slave host.
	 *
	 * @param host
	 *            the slave host.
	 * @return the SlaveThread instance.
	 */
	static SlaveThread createPingSlaveThread(final String host) {
		return new SlaveThread(host, "PING", Collections.<String>emptyList());
	}

	/**
	 * Creates a SlaveThread instance to manage the execution of the
	 * "SHUFFLE_REDUCE" command on the designated slave host.
	 *
	 * @param host
	 *            the slave host.
	 * @return the SlaveThread instance.
	 */
	static SlaveThread createReduceShuffleSlaveThread(final String host, final String key, final String reducedMapFile,
	        final Set<String> unsortedMapfiles) {
		final List<String> params = new ArrayList<>();
		params.add(key);
		params.add(reducedMapFile);
		params.addAll(unsortedMapfiles);
		return new SlaveThread(host, "SHUFFLE_REDUCE", params);
	}

	protected final String command; // the command to execute remotely ("PING",
	                                // "MAP", "SHUFFLE_REDUCE")

	protected final String host; // the (remote) slave host

	protected List<String> output = null; // the result of the slave execution
	                                      // as a list of strings

	protected final List<String> params; // the parameters of the command to
	                                     // execute

	/**
	 * Creates an instance of SlaveThread to mange the specified
	 * command/parameters on the specified slave host.
	 *
	 * @param host
	 *            the slave host.
	 * @param command
	 *            the command to remote execute.
	 * @param params
	 *            the parameters to the command.
	 */
	private SlaveThread(final String host, final String command, final List<String> params) {
		this.host = host;
		this.command = command;
		this.params = params;
	}

	/**
	 * Returns the slave host assigned to this SlaveThread instance.
	 *
	 * @return the slave host.
	 */
	String getHost() {
		return host;
	}

	/**
	 * Returns the output (result) of the remote execution.
	 *
	 * @return the output of the remote execution.
	 */
	List<String> getOutput() {
		return output;
	}

	/**
	 * Reads from the provided input stream until EOF condition reached.
	 *
	 * @param is
	 *            the input stream.
	 * @return the lines read from the input stream as a list.
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	private List<String> read(final InputStream is) throws IOException {
		final List<String> list = new ArrayList<>();
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.length() > 0) {
					list.add(line);
				}
			}
		}
		return list;
	}

	/**
	 * Launches the remote execution of the command on the slave host.
	 *
	 * @param host
	 *            the slave host.
	 * @param command
	 *            the command to remote execute.
	 * @param params
	 *            the parameters to the command.
	 * @throws IOException
	 *             if any I/O error occurred.
	 * @throws InterruptedException
	 *             if an interruption occurred.
	 */
	private List<String> remoteExec(final String host, final String command, final List<String> params)
	        throws IOException, InterruptedException {
		final List<String> cmd = new ArrayList<>();
		if (!DEBUG) {
			cmd.add("/usr/bin/ssh");
			cmd.add(host);
		}
		cmd.add("java");
		cmd.add("-jar");
		cmd.add(SLAVE_JAR);
		cmd.add(command);
		cmd.addAll(params);

		System.err.println("SlaveThread command: " + cmd.toString());

		final ProcessBuilder pb = new ProcessBuilder(cmd.toArray(new String[0]));
		pb.redirectError(Redirect.INHERIT); // Redirect error output from slave
		                                    // process to that of the master
		final Process p = pb.start();
		final List<String> output = read(p.getInputStream());
		return p.waitFor() == 0 ? output : null;
	}

	/**
	 * Called to run in its own dedicated thread. Manages the remote execution
	 * of the command on the slave host.
	 */
	@Override
	public void run() {
		try {
			output = remoteExec(host, command, params);
			if (output != null) {
				System.err.println("Completed sucessfully.");
			} else {
				System.err.println("Failed.");
			}
		} catch (final IOException e) {
			e.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		}
	}

	// private void print(InputStream is) throws IOException {
	// try (BufferedReader reader = new BufferedReader(new InputStreamReader(
	// is))) {
	// String line;
	// while ((line = reader.readLine()) != null) {
	// System.err.println(line);
	// }
	// }
	// }
}
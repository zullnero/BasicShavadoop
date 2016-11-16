package shavadoop;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Implements the Shavadoop slave.
 *
 * @author zull
 *
 */
public class Slave {

	/**
	 * Main entry point.
	 *
	 * @param args
	 *            the arguments : the operation (PING|MAP|SHUFFLE_REDUCE)
	 *            followed by parameters.
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	public static void main(final String[] args) throws IOException {
		if (args.length < 1) {
			throw new IllegalArgumentException("Usage: Slave <command=[PING|MAP|SHUFFLE_REDUCE]> <param>");
		}
		new Slave(args[0], Arrays.asList(args).subList(1, args.length));
		System.exit(0);
	}

	/**
	 * Creates an instance of Slave to process the specified operation.
	 *
	 * @param operation
	 *            the operation to process.
	 * @param params
	 *            the parameters of the operations.
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	Slave(final String operation, final List<String> params) throws IOException {
		System.err.println("Starting [" + operation + " " + params + "]...");
		if ("PING".equals(operation)) {
			ping();
			System.out.println("OK");
		} else if ("MAP".equals(operation)) {
			map(params);
		} else if ("SHUFFLE_REDUCE".equals(operation)) {
			shuffleReduce(params);
		}
		System.err.println("Terminated.");
	}

	/**
	 * Implements the map stage.
	 *
	 * @param params
	 *            the parameters.
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	private void map(final List<String> params) throws IOException {
		if (params == null || params.size() != 1) {
			throw new IllegalArgumentException("Usage: Slave MAP <Sx>");
		}
		final Path SxFile = Paths.get(params.get(0));
		final Path UMxFile = SxFile
				.resolveSibling("UM" + SxFile.getName(SxFile.getNameCount() - 1).toString().substring("S".length()));
		final List<String> lines = Files.readAllLines(SxFile, Charset.defaultCharset());
		final List<String> words = new ArrayList<>();
		for (final String line : lines) {
			for (final String word : line.split("[ \t'.,]+")) {
				if (word.length() > 0) {
					words.add(word + ": 1");
					System.out.println(word + ":" + UMxFile);
				}
			}
		}
		if (words.size() > 0) {
			Files.write(UMxFile, words, Charset.defaultCharset(), new OpenOption[0]);
			System.out.flush();
		}
		return;
	}

	/**
	 * Handles the ping operation; Sleeps for 10 seconds.
	 */
	private void ping() {
		try {
			Thread.sleep(10000);
		} catch (final InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Implements the shuffle/reduce stage.
	 *
	 * @param params
	 *            the parameters.
	 * @throws IOException
	 *             if any I/O error occurred.
	 */
	private void shuffleReduce(final List<String> params) throws IOException {
		if (params == null || params.size() < 3) {
			throw new IllegalArgumentException("Usage: Slave SHUFFLE_REDUCE <key> <RMx> <UMx>...");
		}
		final String key = params.get(0);
		final String RMxFile = params.get(1);
		final String SMxFile = RMxFile.replaceFirst("^R", "S");

		int count = 0;
		final List<String> results = new ArrayList<>();
		for (int i = 2; i < params.size(); i++) {
			final String UMxFile = params.get(i);
			final List<String> lines = Files.readAllLines(Paths.get(UMxFile), Charset.defaultCharset());
			for (final String line : lines) {
				final String[] fields = line.split(":");
				final String word = fields[0].trim();
				final String occurence = fields[1].trim();
				if (word.equalsIgnoreCase(key)) {
					results.add(key + ": 1");
					count += Integer.valueOf(occurence);
				}
			}
		}
		if (results.size() > 0 && SMxFile != null) {
			Files.write(Paths.get(SMxFile), results, Charset.defaultCharset(), StandardOpenOption.CREATE,
					StandardOpenOption.TRUNCATE_EXISTING);
		}
		final String output = key + ":" + count;
		if (output.length() > 0) {
			Files.write(Paths.get(RMxFile), output.getBytes(Charset.defaultCharset()), StandardOpenOption.CREATE,
					StandardOpenOption.TRUNCATE_EXISTING);
		}
		System.out.println(output);
		return;
	}

}

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implements the Shavadoop slave.
 *
 * @author S.Cohard et T.Guzelbodur 
 *
 */
public class Slave {
	public static final boolean DEBUG = false;
	// whether local execution of
	// slaves is turned on (for
	// debugging)
	private final static Set<String> REJECTED_WORDS;
	
	static {
		REJECTED_WORDS = new HashSet<String>();
		REJECTED_WORDS.add("le");
		REJECTED_WORDS.add("ou");
		REJECTED_WORDS.add("aux");
		REJECTED_WORDS.add("de");
		REJECTED_WORDS.add("des");
		REJECTED_WORDS.add("la");
		REJECTED_WORDS.add("les");
		REJECTED_WORDS.add("je");
		REJECTED_WORDS.add("l");
		REJECTED_WORDS.add("nous");
		REJECTED_WORDS.add("tu");
		REJECTED_WORDS.add("il");
		REJECTED_WORDS.add("ils");
		REJECTED_WORDS.add("elle");
		REJECTED_WORDS.add("elles");
		REJECTED_WORDS.add("lui");
		REJECTED_WORDS.add("vous");
		REJECTED_WORDS.add("leur");
		REJECTED_WORDS.add("eux");
		REJECTED_WORDS.add("celui");
		REJECTED_WORDS.add("celle");
		REJECTED_WORDS.add("ceux-là");
		REJECTED_WORDS.add("celui-ci");
		REJECTED_WORDS.add("celui-là");
		REJECTED_WORDS.add("celle-ci");
		REJECTED_WORDS.add("celle-là");
		REJECTED_WORDS.add("ceci");
		REJECTED_WORDS.add("cela");
		REJECTED_WORDS.add("ça");
		REJECTED_WORDS.add("celles-ci");
		REJECTED_WORDS.add("celles-là");
		REJECTED_WORDS.add("mien");
		REJECTED_WORDS.add("nôtre");
		REJECTED_WORDS.add("tien");
		REJECTED_WORDS.add("sien");
		REJECTED_WORDS.add("vôtre");
		REJECTED_WORDS.add("leur");
		REJECTED_WORDS.add("mienne");
		REJECTED_WORDS.add("tienne");
		REJECTED_WORDS.add("sienne");
		REJECTED_WORDS.add("miens");
		REJECTED_WORDS.add("tiens");
		REJECTED_WORDS.add("siens");
		REJECTED_WORDS.add("nôtres");
		REJECTED_WORDS.add("vôtres");
		REJECTED_WORDS.add("leurs");
		REJECTED_WORDS.add("miennes");
		REJECTED_WORDS.add("tiennes");
		REJECTED_WORDS.add("siennes");
		REJECTED_WORDS.add("on");
		REJECTED_WORDS.add("personne");
		REJECTED_WORDS.add("rien");
		REJECTED_WORDS.add("aucun");
		REJECTED_WORDS.add("aucune");
		REJECTED_WORDS.add("nul");
		REJECTED_WORDS.add("nulle");
		REJECTED_WORDS.add("un");
		REJECTED_WORDS.add("une");
		REJECTED_WORDS.add("autre");
		REJECTED_WORDS.add("pas");
		REJECTED_WORDS.add("tout");
		REJECTED_WORDS.add("quelqu");
		REJECTED_WORDS.add("quelque");
		REJECTED_WORDS.add("certains");
		REJECTED_WORDS.add("certaine");
		REJECTED_WORDS.add("certain");
		REJECTED_WORDS.add("certaines");
		REJECTED_WORDS.add("plusieurs");
		REJECTED_WORDS.add("tous");
		REJECTED_WORDS.add("qui");
		REJECTED_WORDS.add("que");
		REJECTED_WORDS.add("quoi");
		REJECTED_WORDS.add("dont");
		REJECTED_WORDS.add("où");
		REJECTED_WORDS.add("lequel");
		REJECTED_WORDS.add("laquelle");
		REJECTED_WORDS.add("duquel");
		REJECTED_WORDS.add("auquel");
		REJECTED_WORDS.add("lesquels");
		REJECTED_WORDS.add("desquels");
		REJECTED_WORDS.add("lesquelles");
		REJECTED_WORDS.add("desquelles");
		REJECTED_WORDS.add("auxquelles");
		REJECTED_WORDS.add("laquelle");
		REJECTED_WORDS.add("à");
		REJECTED_WORDS.add("et");
		REJECTED_WORDS.add("ne");
		REJECTED_WORDS.add("du");
		REJECTED_WORDS.add("en");
		REJECTED_WORDS.add("au");
		REJECTED_WORDS.add("pour");
		REJECTED_WORDS.add("par");
		REJECTED_WORDS.add("se");
		REJECTED_WORDS.add("dans");
		REJECTED_WORDS.add("tous");
		REJECTED_WORDS.add("tout");
		REJECTED_WORDS.add("est");
		REJECTED_WORDS.add("ni");
		REJECTED_WORDS.add("qu");
		REJECTED_WORDS.add("être");
		REJECTED_WORDS.add("ses");
		REJECTED_WORDS.add("si");
		REJECTED_WORDS.add("sont");
		REJECTED_WORDS.add("sa");
		REJECTED_WORDS.add("ii");
		REJECTED_WORDS.add("iii");
		REJECTED_WORDS.add("ier");
		REJECTED_WORDS.add("ce");
		REJECTED_WORDS.add("lorsqu");
		REJECTED_WORDS.add("lorsque");
		
		
		
	}

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
		if (DEBUG) System.err.println("Starting [" + operation + " " + params + "]...");
		if ("PING".equals(operation)) {
			ping();
			System.out.println("OK");
		} else if ("MAP".equals(operation)) {
			map(params);
		} else if ("SHUFFLE_REDUCE".equals(operation)) {
			shuffleReduce(params);
		}
		if (DEBUG) System.err.println("Terminated.");
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
			for ( String word : line.split("\\P{L}+")) {
				word = word.toLowerCase();
				if (word.length() > 1 && !REJECTED_WORDS.contains(word)) {
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

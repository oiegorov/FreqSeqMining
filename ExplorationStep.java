package fr.liglab.bide;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.omg.CORBA.BooleanHolder;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.TIntSet;

public class ExplorationStep extends RecursiveAction {
	private static final boolean USE_FORWARD_DIRECT_CLOSURE = false;
	private final int freqThreshold;
	private Dataset dataset;
	private static Dataset dNegDataset;
	private final int[] currentSequence;
	private final ExplorationStep parent;
	private static int nbClosed = 0;
	private static int nbMax = 0;
	private static int nbCandidates = 0;
	private static int nbNotClosedPruned = 0;
	private static int nbNotClosedNotPruned = 0;
	private static int nbDirectForwardClosures = 0;
	private static boolean debugMode = false;
	private static boolean contrastPruning = false;

	public ExplorationStep(int freqThreshold, Dataset dataset, ExplorationStep parent) {
		this(freqThreshold, dataset, new int[] {}, parent);
	}

	public ExplorationStep(int freqThreshold, Dataset dataset, int[] currentSequence, ExplorationStep parent) {
		super();

		if (debugMode) {
      System.out.print("currentSequence: ");
      for (int i = 0; i < currentSequence.length; i++) {
        System.out.print(currentSequence[i] + " ");
      }
      System.out.println();
		}
		
		this.freqThreshold = freqThreshold;
		this.dataset = dataset;
		this.parent = parent;
    this.currentSequence = currentSequence;
		
	}

	public void compute() {
		nbCandidates++;
		BooleanHolder isForwardClosed = new BooleanHolder(true);
		TIntSet expansions = this.dataset.computeExpansions(freqThreshold, isForwardClosed); // items that appear frequently in the forward space
		// System.out.println(this.dataset + " expansions : " + expansions);
		if (isForwardClosed.value) {
			if (!this.dataset.hasBackExtension(this.currentSequence.length)) {
				if (expansions.isEmpty()) {
					nbMax++;
				}
				// System.out.println(Arrays.toString(this.currentSequence) + "
				// with
				// support " + this.dataset.getSupport()
				// + " is closed, parent is " + this.parent);
				nbClosed++;
				if (nbClosed % 1 == 0) {
//					System.out.println("nbClosed " + nbClosed + " nbMax " + nbMax + " " + this + " ("
//							+ nbNotClosedNotPruned + "-" + nbDirectForwardClosures + ")");
				}
			} else {
				nbNotClosedNotPruned++;
			}
		}
		TIntIterator expIter = expansions.iterator();
		List<RecursiveAction> actions = new ArrayList<RecursiveAction>();
		while (expIter.hasNext()) {
			int exp = expIter.next();
			// System.out.println("expanding " + this + " with " + exp);
			Dataset nextDataset = this.dataset.expand(exp);
			if (!nextDataset.isBackClosurePrunable(this.currentSequence.length + 1)) {
				int[] expandedSeq = Arrays.copyOf(this.currentSequence, this.currentSequence.length + 1);
				expandedSeq[this.currentSequence.length] = exp;
        
				ExplorationStep nextStep = new ExplorationStep(this.freqThreshold, nextDataset, expandedSeq, this);
				actions.add(nextStep);
			} 
			else if (contrastPruning) {
				if (this.currentSequence.length > 0) {
          if (dNegDataset.getSupport(this.currentSequence) > 0) {
  // 					do nothing;
          }
				}
			}
			else {
				nbNotClosedPruned++;
			}
		}
		invokeAll(actions);
	}

	@Override
	public String toString() {
		return "ExplorationStep [currentSequence=" + Arrays.toString(this.currentSequence) + ", support=" + this.dataset
				.getSupport()/*
								 * + (this.parent == null ? ", root" :
								 * ", parent=" + Arrays.toString(parent.
								 * currentSequence))
								 */ + "]";
	}

	public static void main(String[] args) throws IOException {
		
		Options options = new Options();
		CommandLineParser parser = new PosixParser();
		
		options.addOption("debug", false, "Debug mode. One thread is launched. All the candidate patterns are output");
		options.addOption("contrast", true, "Contrast pruning must be performed. The value is the path to Dneg file");

		try {
			CommandLine cmd = parser.parse(options, args);

			if (cmd.getArgs().length != 2 || cmd.hasOption('h')) {
				printMan(options);
			}
			else {
				standalone(cmd);
			}
			
    } catch (ParseException e) {
			printMan(options);
		}
		
	}
	
	private static void standalone(CommandLine cmd) throws IOException {


    if (cmd.hasOption("debug")) {
      debugMode = true;
    }
 
    

		List<Transaction> transactions = new ArrayList<Transaction>();
		BufferedReader br = new BufferedReader(new FileReader(cmd.getArgs()[0]));
		String line;
		while ((line = br.readLine()) != null) {
			if (!line.isEmpty()) {
				String[] sp = line.split("\\s+");
				int[] trans = new int[sp.length];
				for (int i = 0; i < sp.length; i++) {
					trans[i] = Integer.parseInt(sp[i]);
				}
				transactions.add(new Transaction(trans));
			}
		}
		br.close();
		Transaction[] transArray = new Transaction[transactions.size()];
		transactions.toArray(transArray);
		Dataset d = new Dataset(transArray);
		
		// initialise Dneg dataset
    if (cmd.hasOption("contrast")) {
    	contrastPruning = true;
			String pathToDneg = cmd.getOptionValue("contrast");
      transactions = new ArrayList<Transaction>();
      br = new BufferedReader(new FileReader(pathToDneg));
      while ((line = br.readLine()) != null) {
        if (!line.isEmpty()) {
          String[] sp = line.split("\\s+");
          int[] trans = new int[sp.length];
          for (int i = 0; i < sp.length; i++) {
            trans[i] = Integer.parseInt(sp[i]);
          }
          transactions.add(new Transaction(trans));
        }
      }
      br.close();
      transArray = new Transaction[transactions.size()];
      transactions.toArray(transArray);
      dNegDataset = new Dataset(transArray);
		}
    
//    int[] trySeq = {20, 18, 1};
//    int sup = dNegDataset.getSupport(trySeq);
//    System.out.println("support is " + sup);
//    System.exit(0);

		
		long startTime = System.currentTimeMillis();
		ExplorationStep es = new ExplorationStep(Integer.parseInt(cmd.getArgs()[1]), d, null);
		int threadNum = debugMode ? 1 : 8;
		ForkJoinPool mainPool = new ForkJoinPool(threadNum);
		mainPool.invoke(es);
//		System.out.println("END nbClosed " + nbClosed + " nbMax " + nbMax + " " + "numCandidates: " + nbCandidates + " (" + nbNotClosedNotPruned + "-"
//				+ nbNotClosedPruned + "-" + nbDirectForwardClosures + ")");
		long endTime = System.currentTimeMillis();
		System.out.println("exectime: " + (endTime-startTime) + " ms nbClosed: " + nbClosed + " numCandidates: " + nbCandidates + " nbNotClosedPruned: " + nbNotClosedPruned);
		
	}
	
	private static void printMan(Options options) {
		String syntax = "java fr.liglab.bide PATHTOFILE MINSUP";
		String header = "\nOptions are :";
		String footer = "";

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(80, syntax, header, options, footer);
	}	
	
}

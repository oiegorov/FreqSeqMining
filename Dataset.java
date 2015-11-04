package fr.liglab.bide;

import java.util.Arrays;
import java.util.BitSet;

import org.omg.CORBA.BooleanHolder;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

public class Dataset {

	private final Transaction[] transactions;
	private final int support;
	// private final int[] noBackSpaceGeneralThanksToTransIndex;

	public Dataset(Transaction[] transactions) {
		super();
		this.transactions = transactions;
		this.support = transactions.length;
		// this.noBackSpaceGeneralThanksToTransIndex = new int[1];
		// this.noBackSpaceGeneralThanksToTransIndex[0] = -1;
	}

	public Dataset(Transaction[] transactions,
			int support/* , int[] noBackSpaceGeneralThanksToTransIndex */) {
		super();
		this.transactions = transactions;
		this.support = support;
		// this.noBackSpaceGeneralThanksToTransIndex =
		// noBackSpaceGeneralThanksToTransIndex;
	}

	public TIntSet computeExpansions(final int freqThreshold, final BooleanHolder isForwardClosed) {
		BitSet bufferBs = new BitSet();
		final TIntIntMap freq = new TIntIntHashMap();
		for (int i = 0; i < this.support; i++) {
			Transaction t = this.transactions[i];
			t.getPotentialExt(bufferBs);
			int pos = 0;
			while ((pos = bufferBs.nextSetBit(pos)) != -1) {
				freq.adjustOrPutValue(pos, 1, 1);
				pos++;
			}
			bufferBs.clear();
		}
		final TIntSet expansions = new TIntHashSet();
		TIntIntIterator mapiter = freq.iterator();
		while (mapiter.hasNext()) {
			mapiter.advance();
			if (mapiter.value() >= freqThreshold) {
				if (mapiter.value() == this.support) {
					isForwardClosed.value = false;
				}
				expansions.add(mapiter.key());
			}
		}
		return expansions;
	}

	protected final int getSupport() {
		return support;
	}

	public boolean isBackClosurePrunable(final int nbItemsInSeq) {
		BitSet inter = new BitSet();
		BitSet bufferBs = new BitSet();
		// doing it from the end cause it feels more likely to fail there so
		// early termination
		for (int i = nbItemsInSeq - 1; i >= 0; i--) {
			// if (noBackSpaceGeneralThanksToTransIndex[i] == -1) {
			for (int j = 0; j < this.support; j++) {
				Transaction t = this.transactions[j];
				t.getBackSpacePruning(i, bufferBs);
				if (inter.isEmpty()) {
					inter.or(bufferBs);
				} else {
					inter.and(bufferBs);
				}
				bufferBs.clear();
				if (inter.isEmpty()) {
					break;
				}
			}
			if (!inter.isEmpty()) {
				return true;
			}
			// }
			bufferBs.clear();
		}
		return false;
	}

	public boolean hasBackExtension(final int nbItemsInSeq) {
		BitSet inter = new BitSet();
		BitSet bufferBs = new BitSet();
		for (int i = 0; i < nbItemsInSeq; i++) {
			// if (noBackSpaceGeneralThanksToTransIndex[i] == -1) {
			for (int j = 0; j < this.support; j++) {
				Transaction t = this.transactions[j];
				t.getBackSpaceGeneral(i, bufferBs);
				if (inter.isEmpty()) {
					inter.or(bufferBs);
				} else {
					inter.and(bufferBs);
				}
				bufferBs.clear();
				if (inter.isEmpty()) {
					// noBackSpaceGeneralThanksToTransIndex[i] = j;
					break;
				}
			}
			if (!inter.isEmpty()) {
				return true;
			}
			// }
		}
		return false;
	}

	public void compress() {
		// System.out.println("compressing " + this);
		for (int i = 0; i < this.support; i++) {
			Transaction t = this.transactions[i];
			t.compress();
		}
	}

	public Dataset expand(int item) {
		Transaction[] nextTrans = new Transaction[this.transactions.length];
		int writeIndex = 0;
		// int firstRemoved = Integer.MAX_VALUE;
		for (int i = 0; i < this.support; i++) {
			Transaction t = this.transactions[i];
			Transaction projTrans = t.expand(item);
			if (projTrans != null) {
				nextTrans[writeIndex] = projTrans;
				writeIndex++;
			} else {
				// firstRemoved = Math.min(firstRemoved, i);
			}
		}
		// int[] nextNeedsBackSpaceGeneralCheck =
		// Arrays.copyOf(this.noBackSpaceGeneralThanksToTransIndex,
		// this.noBackSpaceGeneralThanksToTransIndex.length + 1);
		// for (int i = 0; i < this.noBackSpaceGeneralThanksToTransIndex.length;
		// i++) {
		// if (nextNeedsBackSpaceGeneralCheck[i] >= firstRemoved) {
		// nextNeedsBackSpaceGeneralCheck[i] = -1;
		// }
		// }
		// nextNeedsBackSpaceGeneralCheck[nextNeedsBackSpaceGeneralCheck.length
		// - 1] = -1;
		Dataset expandedDataset = new Dataset(nextTrans,
				writeIndex/* , nextNeedsBackSpaceGeneralCheck */);
		// System.out.println("building expanded dataset " + expandedDataset);
		return expandedDataset;
	}

	public int[] forwardContinuousClose() {
		int[] closure = null;
		for (int i = 0; i < this.support && (closure == null || (closure.length != 0 && closure[0] != -1)); i++) {
			closure = this.transactions[i].updateClosure(closure);
		}
		if (closure != null && closure.length > 0 && closure[0] != -1) {
			for (int i = 0; i < this.support; i++) {
				for (int j = 0; j < closure.length && closure[j] != -1; j++) {
					this.transactions[i] = this.transactions[i].expand(closure[j]);
				}
			}
		}
		return closure;
	}

	@Override
	public String toString() {
		return "Dataset [transactions=" + Arrays.toString(transactions) + ", support=" + support + "]";
	}

	// CHANGE
	
	public int getSupport(int[] sequence) {
		int support = 0;
		for (int i = 0; i < this.transactions.length; i++) {
			if (this.transactions[i].match(sequence)) {
				support++;
			}
		}
		return support;
	}
	
	
}

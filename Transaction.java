package fr.liglab.bide;

import java.util.Arrays;
import java.util.BitSet;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

public class Transaction {
	public static final int EMPTY = -1;

	final private int[] itemSeq;
	final private int[] consumedFirstPos;
	final private int[] consumedLastPos;
	final private int[] consumedLastStablePos;
	private int realLength;

	public Transaction(int[] itemSeq) {
		super();
		this.itemSeq = itemSeq;
		this.consumedFirstPos = new int[] {};
		this.consumedLastPos = new int[] {};
		this.consumedLastStablePos = new int[] {};
		this.realLength = itemSeq.length;
	}

	public Transaction(int[] itemSeq, int[] consumedFirstPos, int[] consumedLastPos, int[] consumedLastStablePos) {
		super();
		this.itemSeq = itemSeq;
		this.consumedFirstPos = consumedFirstPos;
		this.consumedLastPos = consumedLastPos;
		this.consumedLastStablePos = consumedLastStablePos;
		this.realLength = itemSeq.length;
	}

	public int[] updateClosure(int[] currentClosure) {
		int start;
		if (this.consumedFirstPos.length == 0) {
			start = 0;
		} else {
			start = this.consumedFirstPos[this.consumedFirstPos.length - 1] + 1;
		}
		if (currentClosure == null) {
			int[] closure = new int[this.itemSeq.length - start];
			System.arraycopy(this.itemSeq, start, closure, 0, closure.length);
			return closure;
		} else {
			int y;
			int i;
			for (y = 0, i = start; i < this.itemSeq.length && y < currentClosure.length; i++, y++) {
				if (currentClosure[y] != this.itemSeq[i]) {
					currentClosure[y] = -1;
					return currentClosure;
				}
			}
			if (y != currentClosure.length) {
				currentClosure[y] = -1;
			}
			return currentClosure;
		}
	}

	public void compress() {
		// int writeIndex;
		// int readIndex;
		// int consumedIndex;
		// for (readIndex = 0, writeIndex = 0, consumedIndex = 0; readIndex <
		// this.realLength; readIndex++) {
		// if (this.itemSeq[readIndex] != EMPTY) {
		// this.itemSeq[writeIndex] = this.itemSeq[readIndex];
		// if (consumedIndex < this.consumedPos.length &&
		// this.consumedPos[consumedIndex] == readIndex) {
		// this.consumedPos[consumedIndex] = writeIndex;
		// consumedIndex++;
		// }
		// writeIndex++;
		// }
		// }
		// this.realLength = writeIndex;
	}

	public TIntSet getPotentialExt() {
		TIntSet ext = new TIntHashSet();
		int start;
		if (this.consumedFirstPos.length == 0) {
			start = 0;
		} else {
			start = this.consumedFirstPos[this.consumedFirstPos.length - 1] + 1;
		}
		for (int i = start; i < this.realLength; i++) {
			ext.add(this.itemSeq[i]);
		}
		return ext;
	}

	public void getPotentialExt(BitSet bs) {
		int start;
		if (this.consumedFirstPos.length == 0) {
			start = 0;
		} else {
			start = this.consumedFirstPos[this.consumedFirstPos.length - 1] + 1;
		}
		for (int i = start; i < this.realLength; i++) {
			bs.set(this.itemSeq[i]);
		}
	}

	// first occ
	public TIntSet getBackSpacePruning(int pos) {
		TIntSet ext = new TIntHashSet();
		int start;
		if (pos == 0) {
			start = 0;
		} else {
			start = this.consumedFirstPos[pos - 1] + 1;
		}
		for (int i = start; i < this.consumedFirstPos[pos]; i++) {
			ext.add(this.itemSeq[i]);
		}
		return ext;
	}

	// first occ
	public void getBackSpacePruning(int pos, BitSet bs) {
		int start;
		if (pos == 0) {
			start = 0;
		} else {
			start = this.consumedFirstPos[pos - 1] + 1;
		}
		for (int i = start; i < this.consumedFirstPos[pos]; i++) {
			bs.set(this.itemSeq[i]);
		}
	}

	// last occ
	public TIntSet getBackSpaceGeneral(int pos) {
		TIntSet ext = new TIntHashSet();
		int start;
		if (pos == 0) {
			start = 0;
		} else {
			start = this.consumedFirstPos[pos - 1] + 1;
		}
		for (int i = start; i < this.consumedLastPos[pos]; i++) {
			ext.add(this.itemSeq[i]);
		}
		return ext;
	}

	// last occ
	public void getBackSpaceGeneral(int pos, BitSet bs) {
		int start;
		if (pos == 0) {
			start = 0;
		} else {
			start = this.consumedFirstPos[pos - 1] + 1;
		}
		for (int i = start; i < this.consumedLastPos[pos]; i++) {
			bs.set(this.itemSeq[i]);
		}
	}

	public void retainInBackspace(int pos, TIntSet frequent) {
		// int start;
		// if (pos == 0) {
		// start = 0;
		// } else {
		// start = this.consumedPos[pos - 1] + 1;
		// }
		// for (int i = start; i < this.consumedPos[pos]; i++) {
		// if (!frequent.contains(this.itemSeq[i])) {
		// this.itemSeq[i] = -1;
		// }
		// }
	}

	public void retainInExt(TIntSet frequent) {
		// int start;
		// if (this.consumedPos.length == 0) {
		// start = 0;
		// } else {
		// start = this.consumedPos[this.consumedPos.length - 1] + 1;
		// }
		// for (int i = start; i < this.realLength; i++) {
		// if (!frequent.contains(this.itemSeq[i])) {
		// this.itemSeq[i] = -1;
		// }
		// }
	}

	public Transaction expand(int item) {
		int[] itemSeqNext = this.itemSeq;
		int[] consumedFirstPosNext = Arrays.copyOf(this.consumedFirstPos, this.consumedFirstPos.length + 1);
		int[] consumedLastPosNext = Arrays.copyOf(this.consumedLastPos, this.consumedLastPos.length + 1);
		int[] consumedLastStablePosNext = Arrays.copyOf(this.consumedLastStablePos,
				this.consumedLastStablePos.length + 1);
		int firstOcc = -1;
		int lastOcc = -1;
		int preceedingItemStablePos = -1;
		if (this.consumedFirstPos.length == 0) {
			for (int i = 0; i < this.realLength; i++) {
				if (this.itemSeq[i] == item) {
					if (firstOcc == -1) {
						firstOcc = i;
					}
					lastOcc = i;
				}
			}
		} else {
			int preceedingItem = this.itemSeq[this.consumedFirstPos[this.consumedFirstPos.length - 1]];
			for (int i = this.consumedFirstPos[this.consumedFirstPos.length - 1] + 1; i < this.realLength; i++) {
				if (this.itemSeq[i] == item) {
					if (firstOcc == -1) {
						firstOcc = i;
					}
					lastOcc = i;
				}
				if (this.itemSeq[i] == preceedingItem && firstOcc == -1) {
					// TODO fix this
					preceedingItemStablePos = i;
				}
			}
		}
		if (firstOcc == -1) {
			return null;
		} else {
			consumedFirstPosNext[consumedFirstPosNext.length - 1] = firstOcc;
			consumedLastStablePosNext[consumedLastStablePosNext.length - 1] = firstOcc;
			consumedLastPosNext[consumedLastPosNext.length - 1] = lastOcc;
			for (int i = consumedFirstPosNext.length - 2; i >= 0; i--) {
				if (consumedLastPosNext[i] >= consumedLastPosNext[i + 1]) {
					for (int j = consumedLastPosNext[i + 1] - 1; j >= consumedFirstPosNext[i]; j--) {
						if (this.itemSeq[j] == this.itemSeq[consumedLastPosNext[i]]) {
							consumedLastPosNext[i] = j;
							break;
						}
					}
				} else {
					break;
				}
			}
			if (preceedingItemStablePos != -1
					&& preceedingItemStablePos != this.consumedLastStablePos[this.consumedLastStablePos.length - 1]) {
				consumedLastStablePosNext[consumedLastStablePosNext.length - 2] = preceedingItemStablePos;
				for (int i = consumedLastStablePosNext.length - 3; i >= 0; i--) {
					// if(consumedFirstPosNext[i]==consumedLastPosNext[i]){
					// break;
					// }
					int preceedingItem = this.itemSeq[consumedLastStablePosNext[i]];
					int updatedStablePreceedingPos = -1;
					for (int j = consumedLastStablePosNext[i + 1] - 1; updatedStablePreceedingPos == -1; j--) {
						if (this.itemSeq[j] == preceedingItem) {
							updatedStablePreceedingPos = j;
						}
					}
					if (updatedStablePreceedingPos == consumedLastStablePosNext[i]) {
						break;
					} else {
						consumedLastStablePosNext[i] = updatedStablePreceedingPos;
					}
				}
			}
			return new Transaction(itemSeqNext, consumedFirstPosNext, consumedLastPosNext, consumedLastStablePosNext);
		}
	}

	@Override
	public String toString() {
		return "Transaction [itemSeq=" + Arrays.toString(itemSeq) + ", consumedFirstPos="
				+ Arrays.toString(consumedFirstPos) + ", consumedLastPos=" + Arrays.toString(consumedLastPos)
				+ ", consumedLastStablePos=" + Arrays.toString(consumedLastStablePos) + ", realLength=" + realLength
				+ "]";
	}
	
	// TODO
	public boolean match(int[] sequence) {
		for (int i = sequence.length - 1; i < this.itemSeq.length; i++) {
			if (this.itemSeq[i] == sequence[sequence.length - 1]) {
				if (match(sequence, sequence.length - 2, i - 1)) {
					return true;
				}
			}
		}
		return false;
	}

	public boolean match(int[] sequence, int sequenceIndex, int transactionIndex) {
		if (sequenceIndex == -1) {
			return true;
		}
		for (int i = transactionIndex; i >= 0; i--) {
			if (this.itemSeq[i] == sequence[sequenceIndex]) {
				if (match(sequence, sequenceIndex - 1, i - 1)) {
					return true;
				}
			}
		}
		return false;
	}

	public static void main(String[] args) {
		int[] seq = new int[] { 1, 2, 2, 2, 2, 2, 2, 3, 3, 2 };
		Transaction t = new Transaction(seq);
		Transaction t1 = t.expand(1);
		Transaction t12 = t1.expand(2);
		Transaction t122 = t12.expand(2);
		Transaction t1223 = t122.expand(3);
		System.out.println(t);
		System.out.println(t1);
		System.out.println(t12);
		System.out.println(t122);
		System.out.println(t1223);
		Transaction t1222232 = t.expand(1).expand(2).expand(2).expand(2).expand(2).expand(3).expand(2);
		System.out.println(t1222232);
		int[] seq2 = new int[] { 1, 2, 2, 3, 2, 3 };
		Transaction t123 = (new Transaction(seq2)).expand(1).expand(2).expand(3);
		System.out.println(t123);
		BitSet bs = new BitSet();
		t123.getBackSpacePruning(1, bs);
		System.out.println(bs);
		bs = new BitSet();
		t12.getBackSpacePruning(1, bs);
		System.out.println(bs);
	}
}

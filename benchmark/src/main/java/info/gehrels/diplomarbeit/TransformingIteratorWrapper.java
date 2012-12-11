package info.gehrels.diplomarbeit;

import java.util.Iterator;

public abstract class TransformingIteratorWrapper<A, B> extends IterableIterator<B> {
	private final Iterator<A> backingIterator;

	public TransformingIteratorWrapper(Iterator<A> backingIterator) {
		this.backingIterator = backingIterator;
	}
	@Override
	public final boolean hasNext() {
		return backingIterator.hasNext();
	}

	@Override
	public final B next() {
		return calculateNext(backingIterator.next());
	}

	protected abstract B calculateNext(A next);
}

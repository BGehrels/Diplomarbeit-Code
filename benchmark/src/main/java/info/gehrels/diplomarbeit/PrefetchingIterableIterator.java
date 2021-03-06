package info.gehrels.diplomarbeit;

public abstract class PrefetchingIterableIterator<T> extends IterableIterator<T> {
	protected T next;

	@Override
	public final boolean hasNext() {
		ensureNextIsFetched();
		return next != null;
	}

	@Override
	public final T next() {
		ensureNextIsFetched();
		T nodeToReturn = next;
		next = null;
		return nodeToReturn;
	}

	private final void ensureNextIsFetched() {
		if (next == null) {
			fetchNext();
		}
	}

	protected abstract void fetchNext();
}

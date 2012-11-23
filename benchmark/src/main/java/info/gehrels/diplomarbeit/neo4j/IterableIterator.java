package info.gehrels.diplomarbeit.neo4j;

import java.util.Iterator;

abstract class IterableIterator<T> implements Iterable<T>, Iterator<T> {
	@Override
	public abstract boolean hasNext();

	@Override
	public abstract T next();

	@Override
	public final void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public final Iterator<T> iterator() {
		return this;
	}
}

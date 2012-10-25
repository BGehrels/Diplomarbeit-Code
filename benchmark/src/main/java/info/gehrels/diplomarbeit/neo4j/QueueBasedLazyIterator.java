package info.gehrels.diplomarbeit.neo4j;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

public abstract class QueueBasedLazyIterator<E> implements Iterator<E> {
    protected final Queue<E> next = new LinkedList<>();

    @Override
    public final boolean hasNext() {
        ensureNextElementIsFetched();
        return !next.isEmpty();
    }


    @Override
    public final E next() {
        ensureNextElementIsFetched();
        if (next.isEmpty()) {
            throw new NoSuchElementException();
        }

        return next.remove();
    }

    @Override
    public final void remove() {
        throw new UnsupportedOperationException("remove not implemented");
    }

    protected abstract void ensureNextElementIsFetched();
}

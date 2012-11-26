package info.gehrels.diplomarbeit.flockdb;

import info.gehrels.flockDBClient.PagedNodeIdList;

import java.util.Iterator;

class NonPagedResultList implements Iterable<Long> {
	private final PagedNodeIdList result;

	public NonPagedResultList(PagedNodeIdList result) {
		this.result = result;
	}

	@Override
	public Iterator<Long> iterator() {
		return new Iterator<Long>() {
			PagedNodeIdList currentResultPage = result;
			Iterator<Long> myIterator = result.iterator();
			@Override
			public boolean hasNext() {
				if (myIterator.hasNext()) {
					return true;
				}

				if (currentResultPage.hasNextPage()) {
					try {
						currentResultPage = currentResultPage.getNextPage();
						myIterator = currentResultPage.iterator();
					} catch (Exception e) {
						throw new IllegalStateException(e);
					}
				}

				return myIterator.hasNext();
			}

			@Override
			public Long next() {
				return myIterator.next();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}

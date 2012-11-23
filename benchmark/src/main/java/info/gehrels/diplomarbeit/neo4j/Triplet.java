package info.gehrels.diplomarbeit.neo4j;

class Triplet<A extends Comparable<A>, B extends Comparable<B>, C extends Comparable<C>> implements Comparable<Triplet<A,B,C>>{

	public final A elem1;
	public final B elem2;
	public final C elem3;

	public Triplet(A elem1, B elem2, C elem3) {
		this.elem1 = elem1;
		this.elem2 = elem2;
		this.elem3 = elem3;
	}

	@Override
	public int compareTo(Triplet<A,B,C> o) {
		int result = elem1.compareTo(o.elem1);
		if (result == 0) {
			result = elem2.compareTo(o.elem2);
		}

		if (result == 0) {
			result = elem3.compareTo(o.elem3);
		}
		return result;
	}

	@Override
	public String toString() {
		return elem1 + ", " + elem2 + ", " + elem3;
	}
}

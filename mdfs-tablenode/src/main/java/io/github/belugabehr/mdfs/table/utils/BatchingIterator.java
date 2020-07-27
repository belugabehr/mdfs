package io.github.belugabehr.mdfs.table.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class BatchingIterator<E> implements Iterator<Collection<E>> {

	private final PeekingIterator<E> iter;
	private final Comparator<E> cmp;
	private E currentEntry;

	private BatchingIterator(Iterator<E> iter, Comparator<E> cmp) {
		Objects.requireNonNull(iter);
		this.cmp = Objects.requireNonNull(cmp);

		if (!iter.hasNext()) {
			this.iter = Iterators.peekingIterator(Collections.emptyIterator());
			this.currentEntry = null;
		} else {
			this.iter = Iterators.peekingIterator(iter);
			this.currentEntry = this.iter.peek();
		}
	}

	@Override
	public boolean hasNext() {
		return this.iter.hasNext();
	}

	@Override
	public Collection<E> next() {
		final Collection<E> batch = new ArrayList<>();
		while (this.iter.hasNext()) {
			E entry = this.iter.peek();
			if (this.cmp.compare(this.currentEntry, entry) == 0) {
				batch.add(this.iter.next());
			} else {
				this.currentEntry = entry;
				break;
			}
		}
		return batch;
	}

	public static <T> BatchingIterator<T> batch(Iterator<T> iter, Comparator<T> cmp) {
		return new BatchingIterator<T>(iter, cmp);
	}

	public static <T> BatchingIterator<T> batch(Iterable<T> iter, Comparator<T> cmp) {
		return new BatchingIterator<T>(iter.iterator(), cmp);
	}

}

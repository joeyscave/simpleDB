package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private int count;
    private Map<Field, Integer> countGroup;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield == NO_GROUPING) {
            countGroup = null;
        } else {
            countGroup = new ConcurrentHashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfield == NO_GROUPING) {
            count++;
        } else {
            Field f = tup.getField(gbfield);
            if (!countGroup.containsKey(f)) {
                countGroup.put(f, 1);
            } else {
                countGroup.replace(f, countGroup.get(f) + 1);
            }
        }
    }

    private class aggrIterator implements OpIterator {

        private boolean hasNext;
        private Tuple t;

        public aggrIterator() {
            hasNext = true;
            t = new Tuple(getTupleDesc());
            t.setField(0, new IntField(count));
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            hasNext = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return hasNext;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext) return t;
            throw new NoSuchElementException("aggrIterator: next fail");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
            close();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{what.toString()});
        }

        @Override
        public void close() {
            hasNext = false;
        }
    }

    private class aggrGroupIterator implements OpIterator {

        Iterator<Tuple> it;
        ArrayList<Tuple> tuples;

        public aggrGroupIterator() {
            tuples = new ArrayList<>();
            for (Map.Entry<Field, Integer> entry : countGroup.entrySet()) {
                Tuple t = new Tuple(getTupleDesc());
                t.setField(0, entry.getKey());
                t.setField(1, new IntField(entry.getValue()));
                tuples.add(t);
            }
            it = tuples.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = tuples.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"group", what.toString()});
        }

        @Override
        public void close() {
            it = null;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return (gbfield == NO_GROUPING) ? new aggrIterator() : new aggrGroupIterator();
    }

}

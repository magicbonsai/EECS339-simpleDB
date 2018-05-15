package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    
    private final Predicate predicate;
    private  OpIterator source;
    private boolean isOpen;
    
    public Filter(Predicate p, OpIterator child) {
    	predicate = p;
    	source = child;
    	isOpen = false;
    }

    public Predicate getPredicate() {
    	return predicate;
    }

    public TupleDesc getTupleDesc() {
    	return source.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	 source.open();
    	 super.open();
     	isOpen = true;

    }

    public void close() {
    	source.close();
    	super.close();
    	isOpen = false;

    }

    public void rewind() throws DbException, TransactionAbortedException {
    	source.rewind();
    }

    /**
     * AbstractOpIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	
    	if(!isOpen) {
    		return null;
    	}
    	
    	if(!source.hasNext()) {
    		return null;
    	}
    	
    	Tuple t = source.next();
    	while(!predicate.filter(t) && source.hasNext()) {
    		t = source.next();
    	}
    	if(!source.hasNext() && !predicate.filter(t)) {
    		return null;
    	}
    	return t;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] children = new OpIterator[1];
        children[0] = source;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	source = children[0];
    }

}

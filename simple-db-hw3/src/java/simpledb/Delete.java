package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    
    private  OpIterator source;
    private boolean isOpen;
    private final TransactionId tid;
    private boolean isDeleted;

    
    public Delete(TransactionId t, OpIterator child) {
    	source = child;
    	isOpen = false;
    	tid = t;
    	isDeleted = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return Utility.getTupleDesc(1);
    }

    public void open() throws DbException, TransactionAbortedException {
    	isOpen = true;
    	source.open();
    	super.open();
    }

    public void close() {
    	super.close();
    	source.close();
    	isOpen = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	close();
    	open();

    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	
    	int count = 0;
    	if(isDeleted) { // don't run it twice
    		return null;
    	}
    	
    	while(source.hasNext()) {
    		try {
    			Database.getBufferPool().deleteTuple(tid, source.next());
    		} catch(IOException e) {
    			System.out.println("Delete had I/O failusre.");
    		}
    		++count;
    	}
    	
    	TupleDesc td = Utility.getTupleDesc(1);
    	Tuple out = new Tuple(td);
    	out.setField(0, new IntField(count));

    	isDeleted = true;
    	System.out.println("Returning a value of " + count);
    	return out;
    	
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

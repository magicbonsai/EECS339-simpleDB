package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    
    private  OpIterator source;
    private boolean isOpen;
    private final TransactionId tid;
    private final int tableId;
    private boolean inserted;
    
    public Insert(TransactionId t,OpIterator child, int tableid)
            throws DbException {
    	
    	source = child;
    	tid = t;
    	tableId = tableid;
    	isOpen = false;
    	inserted = false;
    	
    	
    	if(!source.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
    		throw new DbException("schema mismatch in insert op");
    	}
    	
    }

    public TupleDesc getTupleDesc() {
    	return Utility.getTupleDesc(1);
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
    	source.open();
    	isOpen = true;
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	int count = 0;
    	if(inserted) {
    		return null;
    	}
    	

    	while(source.hasNext()) {
    		++count;
    		System.out.println("Inserting one");
    		try { 
    			Database.getBufferPool().insertTuple(tid, tableId, source.next());
    		}
    		catch (IOException e) {
    			System.out.println("IO Exception for insert op");
    		}
    	}
    	
    	TupleDesc td = Utility.getTupleDesc(1);
    	Tuple out = new Tuple(td);
    	out.setField(0, new IntField(count));
    	inserted = true;
    	System.out.println("Insert returning a count of " + count);
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

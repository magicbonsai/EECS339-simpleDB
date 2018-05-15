package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    OpIterator childNode;
    private final int gbFieldIdx;
    private final int aFieldIdx;
    private final Op gbType;
    private TupleDesc dstSchema;
    private Aggregator agg;
    private OpIterator aggItr; 

    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
    	childNode = child;
    	
    	gbFieldIdx = gfield;
    	aFieldIdx = afield;
    	gbType = aop;
    	
    	Type[] attrTypes;
    	String[] attrNames;
    	
    	if(gbFieldIdx == Aggregator.NO_GROUPING) {
    		dstSchema = Utility.getTupleDesc(1);
    	}
    	else {
    		attrTypes = new Type[2];
        	attrNames = new String[2];
        	
        	attrTypes[0] = child.getTupleDesc().getFieldType(gbFieldIdx);
        	attrTypes[1] = Type.INT_TYPE;
        	
        	attrNames[0] = child.getTupleDesc().getFieldName(gbFieldIdx);
        	attrNames[1] = child.getTupleDesc().getFieldName(aFieldIdx);
        	dstSchema = new TupleDesc(attrTypes, attrNames);


    	}
    	System.out.println("Dst schema: " + dstSchema.toString());
    	Type gbType = null;
		if(gbFieldIdx != Aggregator.NO_GROUPING) {
			gbType  = childNode.getTupleDesc().getFieldType(gbFieldIdx);
		}

    	if(childNode.getTupleDesc().getFieldType(aFieldIdx) == Type.INT_TYPE) {
    			agg = new IntegerAggregator(gbFieldIdx, gbType, aFieldIdx, aop);
    	}
    	else {
    		agg = new StringAggregator(gbFieldIdx, gbType, aFieldIdx, aop);
    	}

    	aggItr = null;

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	return gbFieldIdx;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	if(gbFieldIdx == Aggregator.NO_GROUPING) {
    		return null;
    	}
    	return dstSchema.getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	return aFieldIdx;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	return childNode.getTupleDesc().getFieldName(aFieldIdx);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	return gbType;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {

    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	childNode.open();
    	super.open();
    	
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(childNode.hasNext()) {
    		System.out.println("Matrializing agg output");
    		while(childNode.hasNext()) {
    			Tuple tup = childNode.next();
    			agg.mergeTupleIntoGroup(tup);
    		
    		}
    		aggItr = agg.iterator();
    	}
    	
    	if(aggItr.hasNext()) {
    		Tuple t = aggItr.next();
        	return t;
    	}
    	else {
    		return null;
    	}
    	
    	
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	this.close();
    	this.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	System.out.println("Returning dst schema " + dstSchema);
    	return dstSchema;
    }

    public void close() {
    	childNode.close();
    	super.close();
    	aggItr = null;
    	agg.reset();
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] children = new OpIterator[1];
        children[0] = childNode;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	childNode = children[0];
    }
    
}

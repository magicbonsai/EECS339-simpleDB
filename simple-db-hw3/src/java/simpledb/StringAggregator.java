package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbFieldIdx;
    private final Type gbFieldType;
    private final int aFieldIdx;
    private final Op gbType;
    private Map<Field, Integer> aggCount; 
    private TupleDesc dstSchema;
    

    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	gbFieldIdx = gbfield;
    	gbFieldType = gbfieldtype;
    	gbType = what;
    	aFieldIdx = afield;
    	aggCount = new HashMap<Field, Integer>();
    	dstSchema = null;
            	
    	assert(gbType == Aggregator.Op.COUNT);
    }

    
    public void reset() {
    	aggCount.clear();
    }
    
    private void deduceDstSchema(Tuple t) {
    	Type[] attrTypes;
    	String[] attrNames;
    	
    	if(gbFieldIdx == Aggregator.NO_GROUPING) {
    		attrTypes = new Type[1];
        	attrNames = new String[1];
        	
        	attrTypes[0] = Type.INT_TYPE;
        	attrNames[1] = new String(gbType.toString());
        	
    	}
    	else {
    		attrTypes = new Type[2];
        	attrNames = new String[2];
        	
        	attrTypes[0] = t.getTupleDesc().getFieldType(gbFieldIdx);
        	attrTypes[1] = Type.INT_TYPE;
        	
        	attrNames[0] = t.getTupleDesc().getFieldName(gbFieldIdx);
        	attrNames[1] = t.getTupleDesc().getFieldName(aFieldIdx);

    	}
    	dstSchema = new TupleDesc(attrTypes, attrNames);

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	System.out.print("Received tuple " + tup);
    	Field gbValue = new IntField(Aggregator.NO_GROUPING);
    	
    	if(aggCount.isEmpty()) {
    		deduceDstSchema(tup);
    	}
    	
    	if(gbFieldIdx != Aggregator.NO_GROUPING) {
    		gbValue = tup.getField(gbFieldIdx);
    	}
       	
    	if(!aggCount.containsKey(gbValue)) { 
    		aggCount.put(gbValue, 1); 
    
    	}
    	else {
    		aggCount.put(gbValue, aggCount.get(gbValue) + 1);
    	}        
    	System.out.println("Have state " + aggCount.toString());
    }

    public Vector<Tuple> getAggResults() {
    	
    	Vector<Tuple> results = new Vector<Tuple>();
    	    	
    	for(Entry<Field, Integer> entry : aggCount.entrySet()){
    			Tuple t = new Tuple(dstSchema);
    			t.setField(0, entry.getKey());
    			t.setField(1, new IntField(entry.getValue()));
    			results.add(t);
    	}

    	return results;
    	
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new AggregatorIterator(this);
    }

}



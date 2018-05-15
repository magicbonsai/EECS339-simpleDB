package simpledb;
import java.util.*;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private final int gbFieldIdx;
    private final int aFieldIdx;
    private final Op gbType;
    private Map<Field, Integer> aggState;
    private Map<Field, Integer> aggCount; // for avg
    private TupleDesc dstSchema;
    
    
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	gbFieldIdx = gbfield;
    	aFieldIdx = afield;
    	gbType = what;
    	    	
    	aggState = new HashMap<Field, Integer>();
    	aggCount = new HashMap<Field, Integer>();
    	dstSchema = null;
    	
    }

    
    private void deduceDstSchema(Tuple t) {
    	Type[] attrTypes;
    	String[] attrNames;
    	
    	if(gbFieldIdx == Aggregator.NO_GROUPING) {
    		attrTypes = new Type[1];
        	attrNames = new String[1];
        	
        	attrTypes[0] = Type.INT_TYPE;
        	attrNames[0] = new String(gbType.toString());
        	
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
    
    
    public void reset() {
    	aggCount.clear();
    	aggState.clear();
    }
    
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Integer aggInput = ((IntField) tup.getField(aFieldIdx)).getValue();
		Field gbValue = new IntField(Aggregator.NO_GROUPING);

		System.out.println("Adding tuple " + tup.toString() + " agg input " + aggInput);
    	if(gbFieldIdx != Aggregator.NO_GROUPING) {
        	gbValue = tup.getField(gbFieldIdx);
    	} 
    	
    	if(aggState.isEmpty()) {
    		deduceDstSchema(tup);
    	}
 
    	// GROUP BY case
    	switch(gbType) {
			case MIN:
				if(!aggState.containsKey(gbValue) || aggInput < aggState.get(gbValue)) {
					aggState.put(gbValue, aggInput);
				}
				break;
			case MAX:
				if(!aggState.containsKey(gbValue) || aggInput > aggState.get(gbValue)) {
					aggState.put(gbValue, aggInput);					
				}
				break;
			case COUNT:
				if(!aggState.containsKey(gbValue)) {
					aggState.put(gbValue, 1);
				}
				else {
					aggState.put(gbValue, aggState.get(gbValue) + 1);
				}
				break;
			case SUM:
			case AVG:
				if(!aggState.containsKey(gbValue)) {
					aggState.put(gbValue, aggInput);
				}
				else {
					aggState.put(gbValue, aggState.get(gbValue) + aggInput);
				}			
				break;
			default:
				break;
		}

    	
    	if(!aggCount.containsKey(gbValue)) { 
    		aggCount.put(gbValue, 1); 
        	}
    	else {
    		aggCount.put(gbValue, aggCount.get(gbValue) + 1);
    	}
    	
    	
    }

    public Vector<Tuple> getAggResults() {
    	
    	Vector<Tuple> results = new Vector<Tuple>();
    	
    	
    	int count;
    	
    	for(Entry<Field, Integer> entry : aggState.entrySet()){
    		System.out.println("Processing entry " + entry.toString());
    		
    		if(gbType == Op.AVG) {
    			count = aggCount.get(entry.getKey());
    			Tuple t = new Tuple(dstSchema);
    			if(gbFieldIdx != Aggregator.NO_GROUPING) {
    				t.setField(0, entry.getKey());
    				t.setField(1, new IntField(entry.getValue() / count));
    			}
    			else {
    				t.setField(0, new IntField(entry.getValue() / count));
    			}
    			System.out.println("Returning tuple " + t.toString());
    			results.add(t);
    		}
    		else { // SUM, MIN, MAX, COUNT
    			Tuple t = new Tuple(dstSchema);
    			if(gbFieldIdx != Aggregator.NO_GROUPING) {
    				t.setField(0, entry.getKey());
    				t.setField(1, new IntField(entry.getValue()));
    			}
    			else {
    				t.setField(0, new IntField(entry.getValue()));	
    			}
    			System.out.println("Returning tuple " +  t.toString());
    			results.add(t);
    		}
    	}

    	return results;
    	
    }
    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
    	return new AggregatorIterator(this);
    }

}





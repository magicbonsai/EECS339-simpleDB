package simpledb;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    static final int NO_GROUPING = -1;

    /**
     * SUM_COUNT and SC_AVG will
     * only be used in lab7, you are not required
     * to implement them until then.
     * */
    public enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT,
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be
         * needed to compute distributed avg in lab7.
         * */
        SUM_COUNT,
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
         * will be used to compute distributed avg in lab7.
         * */
        SC_AVG;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
        
        public String toString()
        {
        	if (this==MIN)
        		return "min";
        	if (this==MAX)
        		return "max";
        	if (this==SUM)
        		return "sum";
        	if (this==SUM_COUNT)
    			return "sum_count";
        	if (this==AVG)
        		return "avg";
        	if (this==COUNT)
        		return "count";
        	if (this==SC_AVG)
    			return "sc_avg";
        	throw new IllegalStateException("impossible to reach here");
        }
    }

    void reset();
    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup);
    
    public Vector<Tuple> getAggResults();

    /**
     * Create a OpIterator over group aggregate results.
     * @see simpledb.TupleIterator for a possible helper
     */
    public OpIterator iterator();
    
}

class AggregatorIterator implements OpIterator {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final Vector<Tuple> aggResults;
	private Iterator<Tuple> aggItr;

	public AggregatorIterator(Aggregator a) {
		aggResults = a.getAggResults();
		aggItr = aggResults.iterator();
		System.out.println("Agg iterator has " + aggResults.size() + " elements.");

	}

    public boolean hasNext() {
    	if(aggItr != null) {
        	return aggItr.hasNext();
    	}
    	return false;
    }

    public Tuple next() {
    	if(aggItr != null) {
    		return aggItr.next();
    	}
    	return null;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    public void rewind() {
    	aggItr = aggResults.iterator();
    }
    
    public TupleDesc getTupleDesc() {
    	return aggResults.get(0).getTupleDesc();
    }
    
    public void open() {
    	aggItr = aggResults.iterator();
    }
    public void close() {
    	aggItr = null;
    	
    }
    

}

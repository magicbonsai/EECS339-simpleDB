package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int ioCostPerPage;
    private HeapFile hFile;
    private TupleDesc tDesc;
    private int numTuples = 0;
    private HashMap<String, Integer> mins, maxs;
    private HashMap<String, IntHistogram> intHistograms;
    private HashMap<String, StringHistogram> stringHistograms;
    
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here

    	this.ioCostPerPage = ioCostPerPage;
    	hFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    	tDesc = hFile.getTupleDesc();
    	mins = new HashMap<String, Integer>();
    	maxs = new HashMap<String, Integer>();
    	intHistograms = new HashMap<String, IntHistogram>();
    	stringHistograms = new HashMap<String, StringHistogram>();
    	numTuples = 0;
    	Transaction t = new Transaction();
    	TransactionId tid = t.getId();
    	DbFileIterator iter = hFile.iterator(tid);
    	
    	setExtremes(iter, tDesc);
    	
    	for(int i = 0; i < tDesc.numFields(); i++){
    		
    		String fieldName = tDesc.getFieldName(i);
    		if(tDesc.getFieldType(i) == Type.INT_TYPE){
    			IntHistogram iHist = new IntHistogram(NUM_HIST_BINS, mins.get(fieldName), maxs.get(fieldName));
    			this.intHistograms.put(fieldName, iHist);
    		}
    		if(tDesc.getFieldType(i) == Type.STRING_TYPE){
    			StringHistogram sHist = new StringHistogram(NUM_HIST_BINS);
    			this.stringHistograms.put(fieldName, sHist);
    		}
    	}
    	
    	fillHistograms(iter, tDesc);
    }
    
    private void setExtremes(DbFileIterator iter, TupleDesc td)
    {
    	Tuple currentTuple;
    	try {
			iter.open();
	    	while (iter.hasNext())
	    	{
	    		currentTuple = iter.next();
	    		numTuples++;
	    		for (int i = 0; i < td.numFields(); i++)
	    		{	    
	    			String fieldname = td.getFieldName(i);
	    			
	    			if(td.getFieldType(i) == Type.INT_TYPE){
	    				int fieldvalue = ((IntField) currentTuple.getField(i)).getValue();
	    				if (!this.maxs.containsKey(fieldname))
	    					maxs.put(fieldname, fieldvalue);
	    				else 
	    				{
	    					int currentMax = maxs.get(fieldname);
	    					int newMax = (currentMax > fieldvalue) ? currentMax : fieldvalue;
	    					maxs.put(fieldname, newMax);
	    				}
	    				if (!this.mins.containsKey(fieldname))
	    					mins.put(fieldname, fieldvalue);
	    				else 
	    				{
	    					int currentMin = mins.get(fieldname);
	    					int newMin = (currentMin < fieldvalue) ? currentMin : fieldvalue;
	    					mins.put(fieldname, newMin);
	    				}
	    			}

	    		}
	    	}
	    	iter.close();
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
}
    
    private void fillHistograms(DbFileIterator iter, TupleDesc td)
    {
    	Tuple currentTuple;
    	try {
			iter.open();
			while (iter.hasNext())
			{
				currentTuple = iter.next();
				for (int i = 0; i < td.numFields(); i++)
				{
					String fieldname = td.getFieldName(i);
					if(td.getFieldType(i) == Type.INT_TYPE){
						int intTupleValue = ((IntField) currentTuple.getField(i)).getValue();
						this.intHistograms.get(fieldname).addValue(intTupleValue);
					}
					if(td.getFieldType(i) == Type.STRING_TYPE){
						String stringTupleValue = ((StringField) currentTuple.getField(i)).getValue();
						this.stringHistograms.get(fieldname).addValue(stringTupleValue);
					}
					
				}
			}
			iter.close();
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
}
    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return ((HeapFile)this.hFile).numPages() * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
    	
        return (int) (this.numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    	String fieldName = tDesc.getFieldName(field);
    	Type fieldType = constant.getType();
    	if(fieldType == Type.INT_TYPE){
    		int v = ((IntField)constant).getValue();
    		IntHistogram iHisto = this.intHistograms.get(fieldName);
    		return iHisto.estimateSelectivity(op, v);
    	}
    	else {
    		String s = ((StringField)constant).getValue();
    		StringHistogram sHisto = this.stringHistograms.get(fieldName);
    		return sHisto.estimateSelectivity(op, s);
    	}
        
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.numTuples;
    }

}

 package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
	
	private int[] hBuckets;
	private int numBuckets;
	private int max, min;
	private int numTuples = 0;
	private int span;
	
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.min = min;
    	this.max = max;
    	this.numBuckets = buckets;
    	hBuckets = new int[numBuckets];
    	for(int i = 0; i < hBuckets.length; i++){
    		hBuckets[i] = 0;
    	}
    	
    	double n = (double) (max-min + 1) / numBuckets;
    	
    	span = (int) Math.ceil(n);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	if(v == max){
    		hBuckets[numBuckets-1]++;
    	}
    	else if (v == min){
    		hBuckets[0]++;
    	}
    	else {
    		
    		int bIndex = (v - min) / span;
    		hBuckets[bIndex]++;
    	}
    	numTuples++;
    	// some code goes here

    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	int bIndex = (v - min) / span;
    	
    	int height;
    	int left, right;
    	
    	
    	left = bIndex*span + min;
    	right = bIndex*span + min + span - 1;
    	
    	switch(op){
    		case EQUALS:
    		case LIKE:
    			if(v < min || v > max){
    				return 0.0;
    			}
    			else{
    				height = hBuckets[bIndex];
    				return (double)(height/span)/numTuples;
    			}
    		case LESS_THAN:
    			if(v <= min){
    				return 0.0;
    			}
    			if(v > max){
    				return 1.0;
    			}
    			else {
    				height = hBuckets[bIndex];
    				double a = (double) height / numTuples;
    				double b = (double) (v - left) / span;
    				double result = a*b;
    				for(int i = bIndex - 1; i >= 0; i--){
    					int tempHeight = hBuckets[i];
    					double a2 = (double) tempHeight / numTuples;
    					result = result + a2;
    				}
    				return result;
    			}
    		case GREATER_THAN:
    			if(v < min){
    				return 1.0;
    			}
    			if(v > max - 1){
    				return 0.0;
    			}
    			else {
    				height = hBuckets[bIndex];
    				double a = (double) height / numTuples;
    				double b = (double) (right - v) / span;
    				double result = a*b;
    				for(int i = bIndex+1; i < hBuckets.length; i++){
    					int tempHeight = hBuckets[i];
    					double a2 = (double) tempHeight / numTuples;
    					result = result + a2;
    				}
    				return result;
    			}
    		case LESS_THAN_OR_EQ:
    			if(v < min){
    				return 0.0;
    			}
    			if(v >= max){
    				return 1.0;
    			}
    			else {
    				height = hBuckets[bIndex];
    				double a = (double) height / numTuples;
    				double b = (double) (v - left) / span;
    				double result = a*b;
    				for(int i = bIndex - 1; i >= 0; i--){
    					int tempHeight = hBuckets[i];
    					double a2 = (double) tempHeight / numTuples;
    					result = result + a2;
    				}
    				result = result + (double)(height/span)/numTuples;
    				return result;
    			}
    		case GREATER_THAN_OR_EQ:
    			if(v < min){
    				return 1.0;
    			}
    			if(v > max - 1){
    				return 0.0;
    			}
    			else {
    				height = hBuckets[bIndex];
    				double a = (double) height / numTuples;
    				double b = (double) (right - v) / span;
    				double result = a*b;
    				for(int i = bIndex+1; i < hBuckets.length; i++){
    					int tempHeight = hBuckets[i];
    					double a2 = (double) tempHeight / numTuples;
    					result = result + a2;
    				}
    				result = result + (double)(height/span)/numTuples;

    				return result;
    			}
    		case NOT_EQUALS:
    			if(v < min || v > max){
    				return 1.0;
    			}
    			else{
    				height = hBuckets[bIndex];
    				double result = (double)(height/span)/numTuples;
    				return 1.0 - result;
    			}
    		default:
    				return 0.0;

    	}
    	
    	
    	
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String s = "";
        for(int i = 0; i < this.numBuckets; i++){
        	s = s + "bucket " + i + ": ";
        	for(int j = 0; i < this.hBuckets[i]; j++){
        		s = s + "|";
        	}
        	s = s + "\n";
        }
        return s;
    }
}

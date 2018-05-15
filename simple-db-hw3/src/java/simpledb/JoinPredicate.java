package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    
    private final int lhsFieldNo;
    private final int rhsFieldNo;
    private final Predicate.Op operator;
    
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
    	lhsFieldNo = field1;
    	rhsFieldNo = field2;
    	operator = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
    	return t1.getField(lhsFieldNo).compare(operator, t2.getField(rhsFieldNo));
    }
    
    public int getField1()
    {
    	return lhsFieldNo;
    }
    
    public int getField2()
    {
    	return rhsFieldNo;
    }
    
    public Predicate.Op getOperator()
    {
    	return operator;
    }
    
    public String toString() {
    	return "lhs fieldno " + lhsFieldNo + " comparison: " + operator.toString() + " rhs fieldNo " +  rhsFieldNo;
    	
    }
    
    public String toString(Tuple lhs, Tuple rhs) {
    	return lhs.getField(lhsFieldNo) + operator.toString() + rhs.getField(rhsFieldNo) + ": " + this.filter(lhs,  rhs);
    }


}

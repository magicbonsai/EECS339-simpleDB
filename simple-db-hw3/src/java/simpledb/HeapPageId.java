package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

	private final int tableId;
	private final int pageNo;
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tblId, int pgNo) {
    	tableId = tblId;
    	pageNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
    	return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
    	return pageNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
    	return tableId + pageNo * 1000; // provisioning for up to 999 tables

    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
       if(o == null) { return false; }
       if(o.getClass() == this.getClass()) {
    	   HeapPageId id = (HeapPageId) o;
    	   if(id.pageNo == this.pageNo && id.tableId == this.tableId) {
    		   return true;
    	   }
       }
       return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}

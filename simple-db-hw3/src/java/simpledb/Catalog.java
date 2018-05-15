package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

	 public static class TableDesc implements Serializable {

	        private static final long serialVersionUID = 1L;
	        public TupleDesc schema;
	        public String name;
	        public String primaryKey;
	        public DbFile databaseFile;
	        
	        public TableDesc() {
	        	schema = new TupleDesc();
	        	primaryKey = "";
	        	databaseFile = null;
	        	name = "";
	        }

	        public TableDesc(String tname, TupleDesc t, String pkey, DbFile dbfile) {
	        	name = tname;
	        	schema = t;
	        	primaryKey = pkey;
	        	databaseFile = dbfile;
	        	
	        }

	 }
	 
	/**
     * Constructor.
     * Creates a new, empty catalog.
     */
	public Map<Integer, TableDesc> tableMap; // name to table details
	public Catalog() {
		tableMap = new HashMap<Integer, TableDesc>();
	}

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
    	int tableId = file.getId();
    	tableMap.put(tableId, new TableDesc(name, file.getTupleDesc(), pkeyField, file));
    	Vector<Integer> toDelete = new Vector<Integer>();
    	
    	for(Integer tKey : tableMap.keySet()) {
    		String itrName = tableMap.get(tKey).name;
    		if(itrName.equals(name) && tKey != tableId) { // same name, different id
    			toDelete.add(tKey);
    		}
    	}
    	
    	for(int i = 0; i < toDelete.size(); ++i) {
    		 tableMap.remove(toDelete.get(i));
    	}
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String tname) throws NoSuchElementException {
    	if(tname == null) {
    		throw new NoSuchElementException();	
    	}
    	for (Map.Entry<Integer, TableDesc> entry : tableMap.entrySet()) {
    		if(entry.getValue().name.equals(tname)) {
    			return entry.getKey();
    		}
    	
    	}
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    	if(tableMap.containsKey(tableid)) {
    		return tableMap.get(tableid).schema;
    	}
    	
    	throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
    	if(tableMap.containsKey(tableid)) {
    		return tableMap.get(tableid).databaseFile;
    	}
    	
    	throw new NoSuchElementException();
    
    }

    public String getPrimaryKey(int tableid) {
    	if(tableMap.containsKey(tableid)) {
    		return tableMap.get(tableid).primaryKey;
    	}
    	
    	throw new NoSuchElementException();
    }

    public Iterator<Integer> tableIdIterator() {
      return tableMap.keySet().iterator();
    	
    }

    public String getTableName(int id) {
    	if(tableMap.containsKey(id)) {
    		return tableMap.get(id).name;
    	}
    	
    	throw new NoSuchElementException();
     
   }
    
    /** Delete all tables from the catalog */
    public void clear() {
    	tableMap.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}


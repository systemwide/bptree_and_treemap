
/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.lang.Boolean.*;
import static java.lang.System.out;

/****************************************************************************************
 * This class implements relational database tables (including attribute names, domains
 * and a list of tuples.  Five basic relational algebra operators are provided: project,
 * select, union, minus and join. The insert data manipulation operator is also provided.
 * Missing are update and delete data manipulation operators.
 */
public class Table
       implements Serializable
{
    /** Relative path for storage directory
     */
    //private static final String DIR = "store" + File.separator;

    /** Filename extension for database files
     */
    private static final String EXT = ".txt";

    /** Counter for naming temporary tables.
     */
    private static int count = 0;

    /** Table name.
     */
    private final String name;

    /** Array of attribute names.
     */
    private final String [] attribute;

    /** Array of attribute domains: a domain may be
     *  integer types: Long, Integer, Short, Byte
     *  real types: Double, Float
     *  string types: Character, String
     */
    private final Class [] domain;

    /** Collection of tuples (data storage).
     */
    private final List <Comparable []> tuples;

    /** Primary key. 
     */
    private final String [] key;

    /** Index into tuples (maps key to tuple number).
     */
    private final Map <KeyType, Comparable []> index;

    //----------------------------------------------------------------------------------
    // Constructors
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Construct an empty table from the meta-data specifications.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = new ArrayList <> ();
        index     = new TreeMap <> ();       // also try BPTreeMap, LinHashMap or ExtHashMap
        //index     = new LinHashMap <> (KeyType.class, Comparable [].class);

    } // constructor

    /************************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples list.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     * @param _tuples     the list of tuples containing the data
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key,
                  List <Comparable []> _tuples)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = _tuples;
        index     = new TreeMap <> ();       // also try BPTreeMap, LinHashMap or ExtHashMap
    } // constructor

    /************************************************************************************
     * Construct an empty table from the raw string specifications.
     *
     * @param name        the name of the relation
     * @param attributes  the string containing attributes names
     * @param domains     the string containing attribute domains (data types)
     */
    public Table (String name, String attributes, String domains, String _key)
    {
        this (name, attributes.split (" "), findClass (domains.split (" ")), _key.split(" "));

        out.println ("DDL> create table " + name + " (" + attributes + ")");
    } // constructor

    //----------------------------------------------------------------------------------
    // Public Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given attributes.
     * Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @author Layton Hayes
     * @param attributes  the attributes to project onto
     * @return  a table of projected tuples
     */
    public Table project (String attributes)
    {
        out.println ("RA> " + name + ".project (" + attributes + ")");
        String [] attrs     = attributes.split (" ");
        Class []  colDomain = extractDom (match (attrs), domain);
        String [] newKey    = (Arrays.asList (attrs).containsAll (Arrays.asList (key))) ? key : attrs;

        List <Comparable []> rows = new ArrayList <> ();
        
      //Our code
        int index;
        for (Comparable[] tuple : this.tuples) {
            Comparable[] addition = new Comparable[attrs.length];
            index = 0;
            for (int i = 0; i < this.attribute.length; i++) {
                for (String atr : attrs) {
                    if(atr.equals(attribute[i])){
                        addition[index] = tuple[i];
                        index++;
                    }
                }
            }
            rows.add(addition);
        }

        return new Table (name + count++, attrs, colDomain, newKey, rows);
    } // project

    /************************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate  the check condition for tuples
     * @return  a table with tuples satisfying the predicate
     */
    public Table select (Predicate <Comparable []> predicate)
    {
        out.println ("RA> " + name + ".select (" + predicate + ")");

        return new Table (name + count++, attribute, domain, key,
                   tuples.stream ().filter (t -> predicate.test (t))
                                   .collect (Collectors.toList ()));
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value).  Use an index
     * (Map) to retrieve the tuple with the given key value.
     *
     * @author Ben Rotolo
     * @param keyVal  the given key value
     * @return  a table with the tuple satisfying the key predicate
     */
    public Table select (KeyType keyVal) {
        out.println ("RA> " + name + ".select (" + keyVal + ")");

        List <Comparable []> rows = new ArrayList <> ();

        /**Our implementation of select (part1) starts here 
        *
        */
        int index;
       
        for(Comparable[] t : tuples) {
            
            for(index = 0; index < t.length; index++) {
                
                KeyType tempKey = new KeyType(t[index].toString());
            
                // For each element, make an equivalent KeyType, and use the 
                // KeyType method .equals() to compare it to keyVal
                if(keyVal.equals(tempKey) && rows.contains(tempKey) == false) {
                    rows.add(t);
                } // if
            
            } // for
            
        } // for       
        
        return new Table (name + count++, attribute, domain, key, rows);
    } // select
    
    // Select with no arguments - should return the calling table.
    public Table select()
    {
         return this;
    }// select

    /************************************************************************************
     * Union this table and table2.  Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @author Zach Saucier
     * @param table2  the rhs table in the union operation
     * @return  a table representing the union
     */
    public Table union (Table table2)
    {
        out.println ("RA> " + name + ".union (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();
        
        // Add the whole first table to the new table
        for (Comparable[] tuple : this.tuples) {
            rows.add(tuple);
        } 
        
        // Add values from the second table that don't match a tuple in this table
        for (Comparable[] tuple : table2.tuples) {
            Boolean flag = FALSE; //If FALSE, add the row
            
            //Parse all tuples and add them into an ArrayList for later processing
            ArrayList<String> tempRow = new ArrayList<String>();
            for (int j = 0; j < tuple.length; j++) {
                tempRow.add(tuple[j].toString());
            } // for
            
            //For each tuple in table1 parse the fields and change them to String objects,
            //add them to a temporary Comparable[] and check for matches in table1
            for (Comparable[] tuple2 : this.tuples) {
                
                ArrayList<String> tempRow2 = new ArrayList<String>();
                for (int j = 0; j < tuple2.length; j++) {
                    tempRow2.add(tuple2[j].toString()); //Add in each field with .toString()
                } // for
            
                //Checks for matches. If there's a match we set the flag to be FALSE 
                //and we won't add it into the final output table
                if (tempRow.equals(tempRow2)) { 
                    flag = TRUE;
                } // if
                
            } // for
            
            //If flag remains TRUE, add the row into the table
            if (flag == FALSE) {
                rows.add(tuple); 
            } // if
            
        } // for

        return new Table (name + count++, attribute, domain, key, rows);
    } // union

    /************************************************************************************
     * Take the difference of this table and table2.  Check that the two tables are
     * compatible.
     *
     * #usage movie.minus (show)
     *
     * @author Jeff Cardinal
     * @param table2  The rhs table in the minus operation
     * @return  a table representing the difference
     */
    public Table minus (Table table2)
    {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        //For every tuple in table1 go through and check for matches in table2
        for (Comparable[] tuple : this.tuples) {
            
            Comparable[] addition = new Comparable[tuple.length]; //Temp array for row addition
            Boolean flag = TRUE; //If TRUE, add the row
            
            //Parse all tuples and add them into an ArrayList for later processing
            ArrayList<String> tempRow = new ArrayList<String>();
            for (int j = 0; j < tuple.length; j++) {
                tempRow.add(tuple[j].toString());
            } // for
            
            //For each tuple in table2 parse the fields and change them to String objects,
            //add them to a temporary Comparable[] and check for matches in table1
            for (Comparable[] tuple2 : table2.tuples) {
                
                ArrayList<String> tempRow2 = new ArrayList<String>();
                for (int j = 0; j < tuple2.length; j++) {
                    tempRow2.add(tuple2[j].toString()); //Add in each field with .toString()
                } // for
            
                //Checks for matches. If there's a match we set the flag to be FALSE 
                //and we won't add it into the final output table
                if (tempRow.equals(tempRow2)) { 
                    flag = FALSE;
                } // if
                
            } // for
            
            //If flag remains TRUE, add the row into the table
            if (flag == TRUE) {
                addition = tuple;
                rows.add(addition); 
            } // if
            
        } // for
        
        return new Table (name + count++, attribute, domain, key, rows);
    } // minus

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by append "2" to the end of any duplicate attribute name.
     *
     * #usage movie.join ("studioNo", "name", studio)
     *
     * @author Layton Hayes and Jeff Cardinal
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (String attributes1, String attributes2, Table table2)
    {
        String [] t_attrs = attributes1.split (" ");
        String [] u_attrs = attributes2.split (" ");
        
        //set of attributes from table1
        Set<String> my_attrs = new HashSet<>();
        for(String str : this.attribute) my_attrs.add(str);
        
        //set of attributes to join on from table1
        Set<String> t1 = new HashSet<>();
        for(String str : t_attrs) t1.add(str);
        
        //set of attributes to join on from table2
        Set<String> t2 = new HashSet<>();
        for(String str : u_attrs) t2.add(str);
        
        //map to store values for attributes to join on
        Map<String, Comparable> map = new HashMap<>();

        //rows for new table
        List <Comparable []> rows = new ArrayList <> ();
        
        boolean found;
        Comparable[] match = new Comparable[0];
        Comparable[] merge;
        
        //for every row in table1
		for (Comparable[] tuple : this.tuples) {

			found = false;

			for (int i = 0; i < this.attribute.length; i++) {// store values
																// from this row
																// to join on
				if (t1.contains(this.attribute[i])) {
					for (int j = 0; j < t_attrs.length; j++) {
						if (t_attrs[j].equals(this.attribute[i]))
							map.put(u_attrs[j], tuple[i]);
					}
				}
			}

			for (Comparable[] tuple2 : table2.tuples) {// search for a matching
														// row in table2
				found = true;
				for (int i = 0; i < tuple2.length; i++) {
					if (t2.contains(table2.attribute[i]) && !tuple2[i].equals(map.get(table2.attribute[i]))) {
						found = false;
					} // if
				} // for

				if (found) { // if a match is found, store it and stop searching
					match = tuple2;
					break;
				} // if
			} // for

			// if a match is found, join the data from the matching rows and add
			// it to the new table.
			if (found) {
				merge = new Comparable[tuple.length + match.length - t_attrs.length];

				int index = 0;
				for (int i = 0; i < tuple.length; i++) {
					merge[index] = tuple[i];
					index++;
				} // for
				for (int i = 0; i < match.length; i++) {
					if (!t2.contains(table2.attribute[i])) {
						merge[index] = match[i];
						index++;
					}
				} // for
				rows.add(merge);
			} // if
		} // for
              
		// building new attribute and domain arrays for new table.
		String[] new_attribute = new String[this.attribute.length + match.length - t_attrs.length];
		Class[] new_domain = new Class[this.attribute.length + match.length - t_attrs.length];

		int index = 0;
		for (int i = 0; i < this.attribute.length; i++) {// add attributes and
															// domains from
															// table1
			new_attribute[index] = this.attribute[i];
			new_domain[index] = this.domain[i];
			index++;
		}
		for (int i = 0; i < table2.attribute.length; i++) {// add attributes and
															// domains from
															// table two,
															// excluding the
															// ones we're
															// joining on
			if (!t2.contains(table2.attribute[i])) {

				if (my_attrs.contains(table2.attribute[i])) { // if there is an
																// identical
																// attribute in
																// table1,
																// append a "2"
					new_attribute[index] = table2.attribute[i] + "2";

				} else {
					new_attribute[index] = table2.attribute[i];
				} // if

				new_domain[index] = table2.domain[i];

				index++;
			} // if
		} // for

        return new Table(name + count++, new_attribute, new_domain, key, rows);

    } // join

    /************************************************************************************
     * Join this table and table2 by performing an "natural join".  Tuples from both tables
     * are compared requiring common attributes to be equal.  The duplicate column is also
     * eliminated.
     *
     * #usage movieStar.join (starsIn)
     *
     * @author Layton Hayes
     * @param table2  the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (Table table2)
    {
        out.println ("RA> " + name + ".join (" + table2.name + ")");

        List <Comparable []> rows = new ArrayList <> ();

        //our code
        HashSet<String> attrs = new HashSet<>();    //hashset of attributes
        HashSet<String> overlap = new HashSet<>();    //hashset of overlapping attributes
        for (String attr : this.attribute) attrs.add(attr);     //adding attrs from this table
        for (String attr : table2.attribute){//only attrs from table2 not included in this will be added here.
            if(attrs.contains(attr)){
                overlap.add(attr);
            }else{
                attrs.add(attr);
            }
        }

	    //find shorter and longer tables, because length of result will be at most length of shorter
        Table shorter = (this.tuples.size() < table2.tuples.size()) ? this : table2;
        Table longer = (this.tuples.size() >= table2.tuples.size()) ? this : table2;

        Map<String, Comparable> overlapMap;//map to store the values of overlapping attributes, for matching.

        int index;
        for(Comparable[] tuple : shorter.tuples) {//iterate through all rows

            overlapMap = new HashMap<>(overlap.size());
            for (int i = 0; i < this.attribute.length; i++) {//store overlapping attributes
                if(overlap.contains(this.attribute[i])) overlapMap.put(this.attribute[i], tuple[i]);
            }
	    
            Comparable[] tuple2 = new Comparable[0];
            List<Comparable[]> matchList = new ArrayList<>();
            Boolean found = false;
            for (Comparable[] tuple_2 : longer.tuples) {// search for a matching row in the other table
                found = true;
                for (int i = 0; i < tuple_2.length; i++) {
                    //for each attribute, if there is an overlap but the values don't match, then this isn't a match.
                    if(overlap.contains(longer.attribute[i]) && !tuple_2[i].equals(overlapMap.get(this.attribute[i]))) found = false;
                }
                if(found){
                    matchList.add(tuple_2);
                }
            }

            //if this tuple doesn't have a match in the other table, don't include it in the joined table.
            if(!found) continue;

            for(Comparable[] match : matchList) {
                //build new rows from matching rows in the tables
                Comparable[] addition = new Comparable[attrs.size()];
                index = 0;
                for (int i = 0; i < tuple.length; i++) {
                    addition[index] = tuple[i];
                    index++;
                }
                for (int i = 0; i < match.length && index < attrs.size(); i++) {
                    addition[index] = match[i];
                    index++;
                }
                //add the row to the new table
                rows.add(addition);
            }
        }

        //new attributes and domains
        String[] attributes = new String[attrs.size()];
        Class[] domains = new Class[attrs.size()];

        index = 0;
        for(int i = 0; i < this.attribute.length; i++){//add attributes and domains from table1
              attributes[index] = this.attribute[i];
              domains[index] = this.domain[i];
              index++;
        }
        //add attributes and domains from table2 that aren't already included from table1
        for(int i = 0; i < table2.attribute.length && index < attributes.length; i++){
              if(!overlap.contains(table2.attribute[i])){
                     attributes[index] = table2.attribute[i];
                     domains[index] = table2.domain[i];
                     index++;
              }
        }
        //our code

        //changed to attributes and domains below from the concatinations.
        return new Table (name + count++, attributes, domains, key, rows);
    } // join

    /************************************************************************************
     * Return the column position for the given attribute name.
     *
     * @param attr  the given attribute name
     * @return  a column position
     */
    public int col (String attr)
    {
        for (int i = 0; i < attribute.length; i++) {
           if (attr.equals (attribute [i])) return i;
        } // for

        return -1;  // not found
    } // col

    /************************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("'Star_Wars'", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup  the array of attribute values forming the tuple
     * @return  whether insertion was successful
     */
    public boolean insert (Comparable [] tup)
    {
        out.println ("DML> insert into " + name + " values ( " + Arrays.toString (tup) + " )");

        if (typeCheck (tup)) {
            tuples.add (tup);
            Comparable [] keyVal = new Comparable [key.length];
            int [] cols = match (key);
            for (int j = 0; j < keyVal.length; j++) keyVal [j] = tup [cols [j]];
            index.put (new KeyType (keyVal), tup);
            return true;
        } else {
            return false;
        } // if
    } // insert

    /************************************************************************************
     * Get the name of the table.
     *
     * @return  the table's name
     */
    public String getName ()
    {
        return name;
    } // getName

    /************************************************************************************
     * Print this table.
     */
    public void print ()
    {
        out.println ("\n Table " + name);
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
        out.print ("| ");
        for (String a : attribute) out.printf ("%15s", a);
        out.println (" |");
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
        for (Comparable [] tup : tuples) {
            out.print ("| ");
            for (Comparable attr : tup) out.printf ("%15s", attr);
            out.println (" |");
        } // for
        out.print ("|-");
        for (int i = 0; i < attribute.length; i++) out.print ("---------------");
        out.println ("-|");
    } // print

    /************************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex ()
    {
        out.println ("\n Index for " + name);
        out.println ("-------------------");
        for (Map.Entry <KeyType, Comparable []> e : index.entrySet ()) {
            out.println (e.getKey () + " -> " + Arrays.toString (e.getValue ()));
        } // for
        out.println ("-------------------");
    } // printIndex

    /************************************************************************************
     * Load the table with the given name into memory. 
     *
     * @param name  the name of the table to load
     */
    public static Table load (String name)
    {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream (new FileInputStream (name + EXT));
            tab = (Table) ois.readObject ();
            ois.close ();
        } catch (IOException ex) {
            out.println ("load: IO Exception");
            ex.printStackTrace ();
        } catch (ClassNotFoundException ex) {
            out.println ("load: Class Not Found Exception");
            ex.printStackTrace ();
        } // try
        return tab;
    } // load

    /************************************************************************************
     * Save this table in a file.
     */
    public void save ()
    {
        try {
            ObjectOutputStream oos = new ObjectOutputStream (new FileOutputStream (name + EXT));
            oos.writeObject (this);
            oos.close ();
        } catch (IOException ex) {
            out.println ("save: IO Exception");
            ex.printStackTrace ();
        } // try
    } // save

    //----------------------------------------------------------------------------------
    // Private Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e., have
     * the same number of attributes each with the same corresponding domain.
     *
     * @param table2  the rhs table
     * @return  whether the two tables are compatible
     */
    private boolean compatible (Table table2)
    {
        if (domain.length != table2.domain.length) {
            out.println ("compatible ERROR: table have different arity");
            return false;
        } // if
        for (int j = 0; j < domain.length; j++) {
            if (domain [j] != table2.domain [j]) {
                out.println ("compatible ERROR: tables disagree on domain " + j);
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /************************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column  the array of column names
     * @return  an array of column index positions
     */
    private int [] match (String [] column)
    {
        int [] colPos = new int [column.length];

        for (int j = 0; j < column.length; j++) {
            boolean matched = false;
            for (int k = 0; k < attribute.length; k++) {
                if (column [j].equals (attribute [k])) {
                    matched = true;
                    colPos [j] = k;
                } // for
            } // for
            if ( ! matched) {
                out.println ("match: domain not found for " + column [j]);
            } // if
        } // for

        return colPos;
    } // match

    /************************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t       the tuple to extract from
     * @param column  the array of column names
     * @return  a smaller tuple extracted from tuple t 
     */
    private Comparable [] extract (Comparable [] t, String [] column)
    {
        Comparable [] tup = new Comparable [column.length];
        int [] colPos = match (column);
        for (int j = 0; j < column.length; j++) tup [j] = t [colPos [j]];
        return tup;
    } // extract

    /************************************************************************************
     * Check the size of the tuple (number of elements in list) as well as the type of
     * each value to ensure it is from the right domain. 
     *
     * @author Layton Hayes, Ben Rotolo, and Jeff Cardinal
     * @param t  the tuple as a list of attribute values
     * @return  whether the tuple has the right size and values that comply
     *          with the given domains
     */
    private boolean typeCheck (Comparable [] t)
    { 

         if(t.length != this.domain.length) // first check relative size of tuples
         {
             return false;
         }
        
         for(int i = 0; i < t.length; i++){//compare the class of each element to the corresponding domain in the table
             if(!t[i].getClass().equals(this.domain[i])){
                 return false;
             }
         }

        return true; //if everything is on the up and up return true.
    }

    /************************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className  the array of class name (e.g., {"Integer", "String"})
     * @return  an array of Java classes
     */
    private static Class [] findClass (String [] className)
    {
        Class [] classArray = new Class [className.length];

        for (int i = 0; i < className.length; i++) {
            try {
                classArray [i] = Class.forName ("java.lang." + className [i]);
            } catch (ClassNotFoundException ex) {
                out.println ("findClass: " + ex);
            } // try
        } // for

        return classArray;
    } // findClass

    /************************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos the column positions to extract.
     * @param group  where to extract from
     * @return  the extracted domains
     */
    private Class [] extractDom (int [] colPos, Class [] group)
    {
        Class [] obj = new Class [colPos.length];

        for (int j = 0; j < colPos.length; j++) {
            obj [j] = group [colPos [j]];
        } // for

        return obj;
    } // extractDom

} // Table class

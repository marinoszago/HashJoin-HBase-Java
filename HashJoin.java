import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.io.file.tfile.ByteArray;
import java.util.Random;
import java.util.Map.Entry;
import java.util.Set;


import java.util.*;

import java.io.IOException;


public class newTest {

    public static void main(String[] args) {
        /*Create connection for Hbase */
        Configuration conf = HBaseConfiguration.create();
        try {
            Connection conn = ConnectionFactory.createConnection(conf);
            /*Create the admin and connect it with the Hbase*/
            Admin hAdmin = conn.getAdmin();

            /*Procedure to create the tables of Hbase
            First get the names of Tables
            Then get the rows using the getRandomNumberInRange function
            which gives random number of rows between 10-100
            Afterwards create the tables by checking if the tables exist or not with the createOrOverwriteTables function.
             */
            TableName RTable = TableName.valueOf("R");
            TableName STable = TableName.valueOf("S");
            TableName ResultTable = TableName.valueOf("Result");

            int rowsR=getRandomNumberInRange(10,20);
            int rowsS=getRandomNumberInRange(20,100);


            Table tableR = conn.getTable(RTable);
            Table tableS = conn.getTable(STable);
            Table resultT = conn.getTable(ResultTable);

            createOrOverwriteTables(hAdmin,RTable,"R","R1","R2");

            createOrOverwriteTables(hAdmin,STable,"S","S1","S2");

            createOrOverwriteFinalTable(hAdmin,ResultTable,"Result","cf");

            System.out.println("Table created Successfully...");

            /*
            Populate the tables needed for join.
            Using a pseudo random way based on the modulo of i to 4 and 6 respectively
            create the corresponding values of column Key.
             */
            for(int i=1; i<rowsR; i++)
            {
                populateTables(tableR,"R1","R2","Key","Value","userR"+i,""+i%4,""+2*i);

            }
            for(int i=1; i<rowsS; i++)
            {
                populateTables(tableS,"S1","S2","Key","Value","userS"+i,""+i%6,""+2*i+1);

            }
            System.out.println("Table populated Successfully...");


            /*
            Using the function scanData scan the tables of Hbase and the column family and column i want.
             */
            ResultScanner rScanner = scanData(tableR,"R1","Key");
            ResultScanner sScanner = scanData(tableS,"S1","Key");


            byte[] Rrow=null;
            byte[] Rvalue=null;
            byte[] Srow=null;
            byte[] Svalue=null;

            List<String> listOfRowID = new ArrayList<String>();
            List<String> listOfUserR = new ArrayList<String>();

            /* Create the necessary lists of type CustomTuple which contains two Strings
            like <K,V> pairs
             */
            List<CustomTuple> myCustomListR = new ArrayList<CustomTuple>();
            List<CustomTuple> myCustomListS = new ArrayList<CustomTuple>();

            /*
            Fill the lists above so that every list is of rowID,columnKeyValue pairs
             */
            myCustomListR = fillList(rScanner,Rrow,Rvalue,myCustomListR);
            myCustomListS = fillList(sScanner,Srow,Svalue,myCustomListS);

            printData(myCustomListR);
            printData(myCustomListS);

            /*
            Create the Hash Map needed for the hashjoin
            In order to perform the hash join the small relation should be put inside the hash map
            Here the small relation is R
            The corresponding list containing the R is myCustomListR
             */
            HashMap<String,List<String>> hashM = new HashMap<String,List<String>>();
            hashM= putTheSmallRelationIntoHashMap(hashM,listOfRowID,myCustomListR);
            /*
            Perform the hash join between the hash map and the big relation and store the resulting tuples
            inside a Hbase table called Result with one column family and three columns:
            rowIDR,joinedKey,rowIDS
             */
            hashJoin(hashM,myCustomListS,listOfUserR,resultT);

            System.out.println("--------------------------------------------------");
            System.out.println("--------------------------------------------------");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
    Function populateTables:
    Params: Table, Columnfamily1,Columnfamily2,Column1 of cf1,Column2 of cf2, RowID , ColumnValue of Column1, ColumnValue of Column2
    Usage: For each RowID add to the Columnfamilies the Columns and their respective Values
     */
    public static void populateTables(Table t,String cf1,String cf2,String col1,String col2,String rowValue,String colValue1,String colValue2)throws IOException {
        Put p = new Put(Bytes.toBytes(rowValue));

        p.addColumn(Bytes.toBytes(cf1), Bytes.toBytes(col1), Bytes.toBytes(colValue1));
        p.addColumn(Bytes.toBytes(cf2), Bytes.toBytes(col2), Bytes.toBytes(colValue2));

        t.put(p);
    }
    /*
  Function populateFinalTable:
  Params: FinalTable, Columnfamily1Column1 of cf1,Column2 of cf1,Column3 of cf1, RowID , ColumnValue of Column1, ColumnValue of Column2, Columnvalue of Column3
  Usage: For each RowID add to the Columnfamilies the Columns and their respective Values
   */
    public static void populateFinalTable(Table t,String cf1,String col1,String col2,String col3,String rowValue,String colValue1,String colValue2,String colValue3) throws IOException
    {
        Put p = new Put(Bytes.toBytes(rowValue));

        p.addColumn(Bytes.toBytes(cf1), Bytes.toBytes(col1), Bytes.toBytes(colValue1));
        p.addColumn(Bytes.toBytes(cf1), Bytes.toBytes(col2), Bytes.toBytes(colValue2));
        p.addColumn(Bytes.toBytes(cf1), Bytes.toBytes(col3), Bytes.toBytes(colValue3));

        t.put(p);
    }
    /*
    Function createOrOverwriteTables:
    Params: Admin created, TableName to be created,name of table as a String,Columnfamily1 , Columnfamily2
    Usage: Checks if the table is already created . If true overwrites it and creates the table
    */
    public static void createOrOverwriteTables(Admin admin,
                                         TableName table,String tableName,String colFamily1 , String colFamily2) throws IOException {
        if(admin.tableExists(table)){
            admin.disableTable(table);
            admin.deleteTable(table);
        }

        HTableDescriptor hTableDesc = new HTableDescriptor(
                table.valueOf(tableName));

        hTableDesc.addFamily(new HColumnDescriptor(colFamily1));
        hTableDesc.addFamily(new HColumnDescriptor(colFamily2));
        // Create the table
        admin.createTable(hTableDesc);
    }
    /*
   Function createOrOverwriteFinalTable:
   Params: Admin created, TableName to be created,name of table as a String,Columnfamily
   Usage: Checks if the table is already created . If true overwrites it and creates the table
   */
    public static void createOrOverwriteFinalTable(Admin admin,
                                         TableName table,String tableName,String colFamily) throws IOException {
        if(admin.tableExists(table)){
            admin.disableTable(table);
            admin.deleteTable(table);
        }

        HTableDescriptor hTableDesc = new HTableDescriptor(
                table.valueOf(tableName));

        hTableDesc.addFamily(new HColumnDescriptor(colFamily));
        // Create the table
        admin.createTable(hTableDesc);
    }
    /*
   Function scanData:
   Params: Table to be scanned, the column family and the column
   Usage: Scans the Hbase table and returns the scanner with the tables Data
   */
    public static ResultScanner scanData(Table table,String colFamily,String col) throws IOException{
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        ResultScanner scanner = table.getScanner(scan);
        return scanner;
    }
    /*
    Function fillList:
    Params:Scanner, row, valueOfColumnKey,List
    Usage: Fill the list with data from the scanner so that the list is with <RowID,KeyColumnValue> pairs
    */
    public static List<CustomTuple> fillList(ResultScanner rs,byte[] row,byte[] valueOfColumnKey,List<CustomTuple> myList)throws IOException{
        for (Result result = rs.next(); result != null; result = rs.next()){
            for(Cell cell : result.rawCells()) {
                row = CellUtil.cloneRow(cell);
                valueOfColumnKey = CellUtil.cloneValue(cell);

                CustomTuple ct = new CustomTuple(Bytes.toString(row),Bytes.toString(valueOfColumnKey));
                myList.add(ct);
            }
        }
        return myList;
    }
    public static void printData(List<CustomTuple> list)
    {
        System.out.println("Printing Lists using (rowKey,KeyColumn) format: ");
        for (CustomTuple string:list)
        {
            System.out.println("("+string.rowID+","+string.key+")");
        }
    }

    public static int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }
        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }

    public static void printHashMap(HashMap<String,List<String>> hm)
    {
        String variableKey;
        List<String> variableValue = new ArrayList<String>();

        System.out.println("Printing the HashMap...");

        for (String variableName : hm.keySet())
        {
            variableKey = variableName;
            variableValue = hm.get(variableName);

            System.out.println("Key: " + variableKey);
            System.out.println("Values: " + variableValue);
        }
    }
    /*
   Function hashJoin:
   Params: HashMap<String,List<String>>, List of CustomTuple,List for rowID of the small relation, Table to be stored in HBase
   Usage: For every key in HashMap keySet and for every rowID list of this Key check
          If the key of the keySet is equalt to key of the Big Relation(here is list and ct.key)
          If true then call populateFinalTable and populate the HBase Result Table with
          tuples of format: RowIDR,JoinedKey,RowIDS
   */
    public static void hashJoin(HashMap<String,List<String>> hm, List<CustomTuple> list,List<String> rowList,Table t) throws  IOException{
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Probe Phase: Using the bigger Relation check into the HashMap if the Keys join...");
        System.out.println("---------------------------------------------------------------------------------");


        Integer index =0;
        for (String key : hm.keySet()) {
            rowList = hm.get(key);
            for(String s:rowList)
            {
                for (CustomTuple ct : list) {
                    if (ct.key.equals(key)) {
                        index++;
                        populateFinalTable(t, "cf", "RowIDR", "JoinedKey", "RowIDS", "joined"+index,s, ct.key, ct.rowID);

                    }
                }
            }
            rowList = new ArrayList<String>();
        }
    }
    /*
    Function putTheSmallRelationIntoHashMap:
    Params: HashMap<String,List<String>>, List of String,List of CustomTaple, Table to be stored in HBase
    Usage: For every customtuple(ct) in the small relation (here is customList)
           Check if the hashmap value of key list is not empty
           If true get that list and add the customtuple rowID(ct.rowID) to the list
           then add the ct.key and list to the HashMap
*/
    public static HashMap<String,List<String>> putTheSmallRelationIntoHashMap(HashMap<String,List<String>> hashmap, List<String> list,List<CustomTuple> customList)
    {
        for(CustomTuple ct: customList)
        {
            if (hashmap.get(ct.key) == null) {
                list.add(ct.rowID);
                hashmap.put(ct.key, list);
                list = new ArrayList<String>();
            }
            else if (hashmap.get(ct.key).isEmpty()) {
                list.add(ct.rowID);
                hashmap.put(ct.key, list);
                list = new ArrayList<String>();

            } else {
                list = hashmap.get(ct.key);
                list.add(ct.rowID);
                hashmap.put(ct.key, list);
                list = new ArrayList<String>();
            }
        }
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Build Phase: Create HashMap of the small Relation before the join phase...");
        printHashMap(hashmap);
        return hashmap;
    }
}

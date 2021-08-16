import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.*;
import java.io.IOException;
public class DavisBaseDeleteRecord 
{	
    public static void deleteRecord(String deleteString)
    {	
        //check if table is present
        ArrayList<String> deleteTokens = new ArrayList<String>(Arrays.asList(deleteString.split("\s+")));
        //check first part of string to make sure it is well formed
        if(!deleteTokens.get(0).equals("delete") || !deleteTokens.get(1).equals("from") || !deleteTokens.get(2).equals("table"))
        {
            System.out.println("Malformed first part of delete query missing either delete, from, or table");
            System.out.println("USAGE:     delete from table <table_name> where rowid = <row_id>;");
            return;
        }
        String tableName = deleteTokens.get(3);
        
        //check to make sure table is present in db
        Path table = Paths.get("data/" + tableName + ".tbl");
        if(!Files.exists(table))
        {
            System.out.println("Table " + tableName + " is not present in database!");
            return;
        }
        //dont let them touch davisbase_columns or tables
        if(tableName.equals("davisbase_columns")|| tableName.equals("davisbase_tables"))
        {
            System.out.println("You cannot delete from davisbase columns or tables table!");
            return;
        }
        //check for where condition
        if(!deleteString.contains("where") || !deleteString.contains("rowid"))
        {
            System.out.println("Incorrect delete call missing either where or rowid to search on");
            System.out.println("USAGE:     delete from table <table_name> where rowid = <row_id>;");
            return;
        }

        try
        {
            RandomAccessFile tableFile = new RandomAccessFile("data/" + tableName + ".tbl", "rw");
            if(deleteString.contains("<="))
            {
                String id = deleteString.substring(deleteString.lastIndexOf('=') + 1, deleteString.length() ).trim();
                int rowid = Integer.parseInt(id);
                for(int i = 1; i <=rowid; i++)
                {
                    deleteRecordById(i, tableFile);
                }
            }
            else if(deleteString.contains("<"))
            {
                String id = deleteString.substring(deleteString.lastIndexOf('<') + 1, deleteString.length() ).trim();
                int rowid = Integer.parseInt(id);
                for(int i = 1; i <rowid; i++)
                {
                    deleteRecordById(i, tableFile);
                }
            }
            else if(deleteString.contains(">="))
            {
                String id = deleteString.substring(deleteString.lastIndexOf('=') + 1, deleteString.length() ).trim();
                int rowid = Integer.parseInt(id);
                int maxId = DavisBaseDropTable.getRowid(tableFile, (int) (tableFile.length()/DavisBasePrompt.pageSize -1));
                if(maxId <= rowid)
                {
                    return;
                }
                else
                {
                    for(int i = rowid; i<= maxId; i++)
                    {
                        deleteRecordById(i, tableFile);
                    }
                }
            }
            else if(deleteString.contains(">"))
            {
                String id = deleteString.substring(deleteString.lastIndexOf('>') + 1, deleteString.length() ).trim();
                int rowid = Integer.parseInt(id);
                int maxId = DavisBaseDropTable.getRowid(tableFile, (int) (tableFile.length()/DavisBasePrompt.pageSize -1));
                if(maxId <= rowid)
                {
                    return;
                }
                else
                {
                    for(int i = rowid+1; i<= maxId; i++)
                    {
                        deleteRecordById(i, tableFile);
                    }
                }
            }
            else
            {
                String id = deleteString.substring(deleteString.lastIndexOf('=') + 1, deleteString.length() ).trim();
                int rowid = Integer.parseInt(id);
                deleteRecordById(rowid, tableFile);
            }
            System.out.println("Record(s) Deleted!!");
            tableFile.close();
        }
        catch(IOException e)
        {
        	System.out.println(e.getMessage());
        }

        
    }

    public static void deleteRecordById(int rowid, RandomAccessFile tableFile)
    {   try
        {
            int numPages = (int) (tableFile.length()/DavisBasePrompt.pageSize);
            boolean found = false;
            //find record from all the pages
            for(int i =0; i<numPages; i++)
            {
                //read num recs
                tableFile.seek(i*DavisBasePrompt.pageSize + 2);
                short numRecs = tableFile.readShort();
                //rec pointers start at 16
                short spot = 16;
                short recPointer;
                int recId;
                for(short j =0; j<numRecs; j++)
                {
                    tableFile.seek(i*DavisBasePrompt.pageSize + spot);
                    recPointer = tableFile.readShort();
                    tableFile.seek(recPointer + i*DavisBasePrompt.pageSize + 2);
                    recId = tableFile.readInt();
                    //check to see if recId == rowid if so decrement record and check to make sure pointers are good
                    if(rowid == recId)
                    {
                        //decrement num recs
                    	
                        found = true;
                        tableFile.seek(recPointer);
                        tableFile.writeShort(0);
                        //need to move pointers down
                        DavisBaseDropTable.decrementRecords(j,numRecs,i,tableFile);
                        //decrement record count 
                        tableFile.seek(i*512+0x02);
                        numRecs = (short) (numRecs -1);
                        tableFile.writeShort(numRecs);
                        
                        //realign most recent pointer
                        DavisBaseDropTable.changeMostRecentPointer(i,tableFile);
                        break;
                    }
                    if(found == true){
                    	break;}

                    //increment to next rec
                    spot = (short) (spot + 2);
                }
            }
            
            if(found == false) {
            	System.out.println("Record Not Found!! Nothing to delete!");
            }
           
        }
        catch(IOException e)
        {

        }
    }
}
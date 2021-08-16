import java.io.RandomAccessFile;
import java.nio.file.*;
import java.io.IOException;

public class DavisBaseDropTable 
{
    public static void dropTable(String file)
    {
        //1 check to make sure database is valid state
        Path dir = Paths.get("data");
		Path table = Paths.get("data/davisbase_tables.tbl");
		Path columns = Paths.get("data/davisbase_columns.tbl");
		Path fileToBeDeleted = Paths.get("data/"+ file+".tbl");

		if( !Files.exists(fileToBeDeleted)) {
			System.out.println("Table " + file +" does not exist in the database. Nothing to drop!");
		}
		else {
			

			if(!Files.isDirectory(dir) || !Files.exists(table) || !Files.exists(columns))
			{
				System.out.println("db is not initilized properly cannot drop table");
	            return;
			}
	        if(file.equals("davisbase_tables") || file.equals("davisbase_columns"))
	        {
	            System.out.println("Cannot remove " + file + "as it is required for database");
	            return;
	        }
	        //Attempt to delete file
	        try
	        {
	            Files.deleteIfExists(Paths.get("data/" + file + ".tbl"));
	        }
	        catch(NoSuchFileException e)
	        {
	            System.out.println("Table is not in database therefore cannot delete it");
	            return;
	        }
	        catch(DirectoryNotEmptyException e)
	        {
	            System.out.println("Directory is not empty.");
	        }
	        catch(IOException e)
	        {
	            System.out.println("Invalid permissions.");
	        }

	        //Need to delete record from table_file
	        try
	        {
	            RandomAccessFile davisTable = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
	            //handling linked list style file
	            boolean found = false;
	            int numPages = (int)davisTable.length()/512;
	            short spot;
	            byte [] tableName;
	            for(int x = 0; x<numPages; x++ )
	            {
	                //read number of records from a page
	                davisTable.seek(x*512 + 2);
	                short numRecords = davisTable.readShort();
	                //get ready to read first record
	                int record = 0x10 + x*512;
	                for(short j = 0; j<numRecords ; j++)
	                {
	                    davisTable.seek(record);
	                    spot = davisTable.readShort();
	                    //record header is 6 bytes numcolumn is 1 byte column length is 2 bytes 
	                    //reading length of the string
	                    int length;
	                    davisTable.seek(spot+7 + x*512);
	                    length = (davisTable.readByte()  & 0xFF) -12; 
	                    tableName = new byte[length];
	                    davisTable.read(tableName,0,length);
	                    String tbName = new String(tableName);
	                    //if the value == file then remove pointer
	                    //reduce value of records on page
	                    //check to see if it in the pointer most recent record if it is fix it
	                    if(tbName.equals(file))
	                    {
	                        
	                        //pointer removed
	                        found = true;
	                        davisTable.seek(record);
	                        davisTable.writeShort(0);
	                        //need to move pointers down
	                        decrementRecords(j,numRecords,x,davisTable);
	                        //decrement record count 
	                        davisTable.seek(x*512+0x02);
	                        numRecords = (short) (numRecords -1);
	                        davisTable.writeShort(numRecords);
	                        
	                        //realign most recent pointer
	                        changeMostRecentPointer(x,davisTable);
	                        break;
	                    }
	                    if(found == true){break;}
	                    record = record + 2;
	                }
	            }
	            davisTable.close();
	            
	        }
	        catch(IOException e)
	        {

	        }
	        
	        //delete records from davisbase_columns
	        try
	        {
	            RandomAccessFile davisColumns = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
	            int numPages = (int)davisColumns.length()/512;
	            byte [] tableName;
	            short spot;
	            for(int x = 0; x < numPages; x++)
	            {
	                davisColumns.seek(x*512 + 2);
	                short numRecords = davisColumns.readShort();

	                int record = 0x10 + x*512;
	                for(short j = 0; j<numRecords; j++)
	                {
	                    davisColumns.seek(record);
	                    spot = davisColumns.readShort();
	                    // 6 for page header 1 for numcolumns 
	                    davisColumns.seek(spot + 7 + x*512);
	                    int length = (davisColumns.readByte()  & 0xFF) -12; 
	                    tableName = new byte[length];
	                    davisColumns.seek(spot + 12 +x*512);
	                    davisColumns.read(tableName,0,length);
	                    String tbName = new String(tableName);
	                    //if the value == file then remove pointer
	                    //reduce value of records on page
	                    //check to see if it in the pointer most recent record if it is fix it
	                    if(tbName.equals(file))
	                    {
	                        
	                        //pointer removed
	                        davisColumns.seek(record);
	                        davisColumns.writeShort(0);
	                        decrementRecords(j, numRecords, x, davisColumns);
	                        //decrement record count 
	                        davisColumns.seek(0x02 +x*512);
	                        numRecords = (short) (numRecords -1);
	                        davisColumns.writeShort(numRecords);

	                        //check recent record pointer
	                        changeMostRecentPointer(x, davisColumns);
	                        record = record - 2;
	                        j--;
	                        
	                    }

	                    record = record + 2;
	                }
	               
	            }
	            System.out.println(file+ " Table Dropped!!");
	            davisColumns.close();
	            
	        }
	        catch(IOException e)
	        {

	        }
	        

		}
		

        
    }
    public static void changeMostRecentPointer(int page, RandomAccessFile file)
    {
        try
        {
            file.seek(page*DavisBasePrompt.pageSize + 2);
            short numRecs = file.readShort();
            short pointer;
            file.seek(page*DavisBasePrompt.pageSize + 16 + (numRecs-1)*2);
            pointer = file.readShort();
            file.seek(page*DavisBasePrompt.pageSize + 4);
            file.writeShort(pointer);
        }
        catch(IOException e)
        {

        }
    }
    public static void decrementRecords(int deleted, int total, int page, RandomAccessFile file)
    {
        try
        {
            for(int i = deleted ;i<total; i++)
            {
                file.seek(page*DavisBasePrompt.pageSize + 16 + (i+1)*2);
                short next = file.readShort();
                file.seek(page*DavisBasePrompt.pageSize + 16 + i*2);
                file.writeShort(next);
            }
        }
        catch(IOException e)
        {

        }
    }

    public static int getRowid(RandomAccessFile table ,int numPages) throws IOException
    {
        
        if(numPages == 0)
        {
            table.seek(4);
            short lastRec = table.readShort();
            if(lastRec == 0)
            {
                return 0;
            }
            else
            {
                table.seek(lastRec + 2);
                return table.readInt() ;
            }

        }
        else
        {
            table.seek(4 + numPages*DavisBasePrompt.pageSize);
            short lastRec = table.readShort();
            if(lastRec == 0)
            {
                return getRowid(table,numPages-1);
            }
            else
            {
                table.seek(lastRec + 2);
                return table.readInt() ;
            }
        }
        
    }
}
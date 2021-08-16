import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.*;
import java.io.IOException;

public class DavisBaseUpdateRecord
{
    public static void updateRecord(String updateCommand)
    {
        String logicType;
        ArrayList<String> updateTokens = new ArrayList<String>(Arrays.asList(updateCommand.split(" ")));
        String tableName = updateTokens.get(1);
        //make sure update and set are present
        if(!updateTokens.get(0).equals("update") || !updateTokens.get(2).equals("set"))
        {
            System.out.println("Missing update or set term of update record command");
            return;
        }
        //make sure where and rowid are present
        if(!updateCommand.contains("where") || !updateCommand.contains("rowid"))
        {
            System.out.println("Missing where or rowid for update command");
            return;
        }
        //make sure table is present
        Path tablePath = Paths.get("data/" + tableName + ".tbl");
        if(!Files.exists(tablePath) )
        {
            System.out.println(tableName + " table does not exsist in database");
            return;
        }
        //dont let them update columns/tables
        if(tableName.equals("davisbase_columns") || tableName.equals("davisbase_tables"))
        {
            System.out.println("Not allowed to update tables or columns tabbles");
            return;
        }
        //parse out the logic type 
        if(updateCommand.contains("<="))
        {
            updateCommand = updateCommand.replaceAll("<="," ");
            logicType = "<=";
        }
        else if(updateCommand.contains(">="))
        {
            updateCommand = updateCommand.replaceAll(">="," ");
            logicType = ">=";
        }
        else if(updateCommand.contains("<"))
        {
            updateCommand = updateCommand.replaceAll("<"," ");
            logicType = "<";
        }
        else if(updateCommand.contains(">"))
        {
            updateCommand = updateCommand.replaceAll(">"," ");
            logicType = ">";
        }
        else
        {
            logicType ="=";
        }
        updateCommand = updateCommand.replaceAll("="," ");
        updateCommand = updateCommand.replaceAll("\\s{2,}", " ");
        //Need to parse between Set and Where
        ArrayList<String> getUpdateTokens;
        String fieldValues = updateCommand.substring(updateCommand.indexOf("set")+3, updateCommand.indexOf("where")-1).trim();
        //should have all rows with values storred into array list
        if(fieldValues.contains(", "))
        {
            getUpdateTokens = new ArrayList<String>(Arrays.asList(fieldValues.split(", ")));
        }
        else
        {
            getUpdateTokens = new ArrayList<String>(Arrays.asList(fieldValues.split(",")));
        }
        
        //parse out rowid
        String rowidText = updateCommand.substring(updateCommand.indexOf("rowid")+5, updateCommand.length()).trim();
        int rowid = Integer.parseInt(rowidText);


        
        //retrive informtion about table from davisbase_columns
        Map<String, Column> columnMap = new HashMap<String, Column>();
        Map<Integer, String> ordinalMap = new HashMap<Integer, String>();
        try
        {
            RandomAccessFile tableFile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
            int numPages = (int) (tableFile.length()/DavisBasePrompt.pageSize);
            for(int i=0; i<numPages; i++)
            {
                tableFile.seek(i*DavisBasePrompt.pageSize + 2);
                short numRecs = tableFile.readShort();
                int spot = 16;
                for(int j = 0; j<numRecs; j++)
                {
                    byte ordinal;
                    String type;
                    String rowName;
                    String mytable;
                    tableFile.seek(i*DavisBasePrompt.pageSize + spot);
                    short recPointer = tableFile.readShort();
                    //+7 = table name +8 = row name +9 = type + 10 = ordinal + 11 = not null
                    //read table name and setup other variable
                    tableFile.seek(i*DavisBasePrompt.pageSize + recPointer+7);
                    int lengthOfTable = (tableFile.readByte()  & 0xFF) -12;
                    //int lengthOfTable = (tableFile.readByte()  & 0xFF); 
                    byte [] tableBytes = new byte[lengthOfTable];

                    int lengthOfColumn = (tableFile.readByte()  & 0xFF) -12;
                    //int lengthOfColumn = (tableFile.readByte()  & 0xFF); 
                    byte [] columnBytes = new byte[lengthOfColumn];

                    int lengthOfType = (tableFile.readByte()  & 0xFF) -12;
                    //int lengthOfType = (tableFile.readByte()  & 0xFF);
                    byte [] typeBytes = new byte[lengthOfType];

                    tableFile.readByte(); // just skipping over the ordinal byte

                    int lengthOfNull = (tableFile.readByte()  & 0xFF) -12;
                    //int lengthOfNull = (tableFile.readByte()  & 0xFF); 

                    tableFile.read(tableBytes,0,lengthOfTable);
                    mytable = new String(tableBytes);
                    //supplied table name == read table name
                    if(mytable.equals(tableName))
                    {
                        tableFile.read(columnBytes,0,lengthOfColumn);
                        rowName = new String(columnBytes);

                        tableFile.read(typeBytes,0,lengthOfType);
                        type = new String(typeBytes);

                        ordinal = tableFile.readByte();

                        if(lengthOfNull > 2)
                        {
                            Column column = new Column(mytable,rowName,type,"yes",(int) ordinal);
                            columnMap.put(rowName,column);
                            ordinalMap.put((int)ordinal,type);
                        }
                        else
                        {
                            Column column = new Column(mytable,rowName,type,"no",(int) ordinal);
                            columnMap.put(rowName,column);
                            ordinalMap.put((int)ordinal,type);
                        }
                    }
                    spot = spot + 2;
                }
            }
            tableFile.close();
        }
        catch(IOException e)
        {

        }
        
        
        //getUpdateTokens spot 0 = column spot 1 = value
        Update[] updateArray = new Update[columnMap.size()];
        for(int i = 0; i<getUpdateTokens.size();i++)
        {
            String parseColumn = getUpdateTokens.get(i);
            ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(parseColumn.split(" ")));
            
            Column temp = columnMap.get(columnTokens.get(0));
            if(temp == null)
            {
                System.out.println("Entered " + columnTokens.get(0) + " which table " + tableName + " doesn't have a column for");
                return;
            }
            else
            {
                updateArray[temp.ordinal] = new Update(temp.type,columnTokens.get(1),temp.ordinal);
                if(temp.type.equals("text"))
                {
                    updateArray[temp.ordinal].length = updateArray[temp.ordinal].value.length();
                }
            } 
            
        }
        
        //sort list for ease of use
        for(int i = 0; i<updateArray.length;i++)
        {
            for(int j =i+1; j<updateArray.length; j++)
            {
                if(updateArray[i] != null && updateArray[j] != null && updateArray[i].ordinal > updateArray[j].ordinal)
                {
                    Update temp = updateArray[i];
                    updateArray[i] = updateArray[j];
                    updateArray[j] = temp;
                }
            }
        }
        //pass in tableName, updateArray, ordinal Map, column map, and rowid
        ///////////////////////////////////////////////////////////////////////////////////////////
        // Making this work for all operators
        //////////////////////////////////////////////////////////////////////////////////////////
        //update array holds the values with type and ordinal skips rowid and starts at 0 find record
        int maxRowid = 0;
        switch(logicType)
        {
            case"<=":
                try {
                    RandomAccessFile checkFile = new RandomAccessFile("data/"+tableName+".tbl","rw");
                    maxRowid = DavisBaseDropTable.getRowid(checkFile,(int) (checkFile.length()/DavisBasePrompt.pageSize - 1));
                    checkFile.close();
                } catch (IOException e) {}
                for(int i = 1; i<=rowid && i <= maxRowid; i++)
                {
                    updateRecordById(tableName, updateArray, columnMap, ordinalMap, i);
                }
                break;
            case"<":
                try {
                    RandomAccessFile checkFile = new RandomAccessFile("data/"+tableName+".tbl","rw");
                    maxRowid = DavisBaseDropTable.getRowid(checkFile,(int) (checkFile.length()/DavisBasePrompt.pageSize - 1));
                    checkFile.close();
                } catch (IOException e) {}
                for(int i = 1; i<rowid && i <= maxRowid; i++)
                {
                    updateRecordById(tableName, updateArray, columnMap, ordinalMap, i);
                }
                break;
            case">":
                try {
                    RandomAccessFile checkFile = new RandomAccessFile("data/"+tableName+".tbl","rw");
                    maxRowid = DavisBaseDropTable.getRowid(checkFile,(int) (checkFile.length()/DavisBasePrompt.pageSize - 1));
                    checkFile.close();
                } catch (IOException e) {}
                for(int i = rowid +1; i<=maxRowid; i++)
                {
                    updateRecordById(tableName, updateArray, columnMap, ordinalMap, i);
                }
                break;
            case">=":
                try {
                    RandomAccessFile checkFile = new RandomAccessFile("data/"+tableName+".tbl","rw");
                    maxRowid = DavisBaseDropTable.getRowid(checkFile,(int) (checkFile.length()/DavisBasePrompt.pageSize - 1));
                    checkFile.close();
                } catch (IOException e) {}
                for(int i = rowid; i<=maxRowid; i++)
                {
                    updateRecordById(tableName, updateArray, columnMap, ordinalMap, i);
                }
                break;
            default:
                updateRecordById(tableName, updateArray, columnMap, ordinalMap, rowid);
        }
       
    }

    public static void setupPage(int Page ,RandomAccessFile file) throws IOException
    {
        file.seek(Page*DavisBasePrompt.pageSize);
        file.writeByte(0x0D);
        file.seek(2);
        file.writeShort(0);
        file.writeShort(0);
        file.writeInt(-1);
        file.writeInt(-1);
    }
    
    public static void updateRecordById(String tableName, Update[] updateArray,Map<String, Column> columnMap,Map<Integer, String> ordinalMap, int rowid )
    {
        Update [] record = new Update [columnMap.size()];
        try
        {
            RandomAccessFile tableFile = new RandomAccessFile("data/" + tableName + ".tbl","rw");
            boolean found = false;
            int numPages = (int) (tableFile.length()/DavisBasePrompt.pageSize);
            for(int i=0; i<numPages; i++)
            {
                tableFile.seek(i*DavisBasePrompt.pageSize + 2);
                short numRecs = tableFile.readShort();
                int spot = 16;
                for(int j = 0; j<numRecs; j++)
                {
                    tableFile.seek(i*DavisBasePrompt.pageSize + spot);
                    short recPointer = tableFile.readShort();
                    //read rowid
                    tableFile.seek(i*DavisBasePrompt.pageSize + recPointer+2);
                    int recId = tableFile.readInt(); 
                    //supplied table name == read table name
                    //i*DavisBasePrompt.pageSize + recPointer+7 is baseline
                    int z = 0;
                    if(rowid == recId)
                    {
                        found = true;
                        int numFields = tableFile.readByte() & 0xFF;
                        for(int k = 0; k<numFields;k++)
                        {
                            
                            int length;
                            String updateType = ordinalMap.get(k);
                            if(updateType.equals("text"))
                            {
                                length = (tableFile.readByte() & 0xFF) - 12;
                                //length = tableFile.readByte() & 0xFF;
                            }
                            else 
                            {
                                tableFile.readByte();
                                length = 0;
                            }
                            record[z] = new Update(updateType,length);
                            z = z +1;

                        }
                        
                        for(int k =0; k<numFields; k++)
                        {
                            byte[] readMe;
                            switch(record[k].type){
                            case"null":
                                break;
                            case "tinyint":
                                record[k].value = Byte.toString(tableFile.readByte());
                                break;
                            case "smallint":
                                record[k].value = Short.toString(tableFile.readShort());
                                break;
                            case "int":
                                record[k].value = Integer.toString(tableFile.readInt());
                                break;
                            case "bigint":
                                record[k].value = Long.toString(tableFile.readLong());
                                break;
                            case "long":
                                record[k].value = Long.toString(tableFile.readLong());
                                break;
                            case "float":
                                record[k].value = Float.toString(tableFile.readFloat());
                                break;
                            case "double":
                                record[k].value = Double.toString(tableFile.readDouble());
                                break;
                            case "year":
                                record[k].value = Byte.toString(tableFile.readByte());
                                break;
                            case "time":
                                record[k].value = Integer.toString(tableFile.readInt());
                                break;
                            case "datetime":
                                record[k].value = Long.toString(tableFile.readLong());
                                break;
                            case "date":
                                readMe = new byte [8];
                                tableFile.read(readMe,0,8);
                                record[k].value = new String(readMe);
                                break;
                            case "text":
                                readMe = new byte [record[k].length];
                                tableFile.read(readMe,0,record[k].length);
                                record[k].value = new String(readMe);
                                break;
                            }
                        }
                        //delete record
                        DavisBaseDeleteRecord.deleteRecordById(rowid, tableFile);
                        break;
                    }
                    spot = spot + 2;
                }
                if(found == true){break;}
            }
            //System.out.println("Could not find record with rowid " + rowid);
            tableFile.close();
            if(found != true)
            {
                return;
            }
        }
        catch(IOException e)
        {

        }
        //Compare update with what was present
        for(int i = 0; i<record.length; i++)
        {
            if(updateArray[i]!= null)
            {
                record[i] = updateArray[i];
            }
        }
        
        //write record to db
        try
        {
            RandomAccessFile tableFile = new RandomAccessFile("data/" + tableName + ".tbl", "rw");
            int numPages = (int) (tableFile.length()/DavisBasePrompt.pageSize -1);
            tableFile.seek(numPages*DavisBasePrompt.pageSize + 2);
            short numRecs = tableFile.readShort();
            short recPointer = tableFile.readShort();
            int payload = 1;
            for(int i = 0; i<record.length; i++)
            {
                payload = payload +1;
                switch(record[i].type){
                    case"null":
                        break;
                    case "tinyint":
                        payload= payload + 1;
                        break;
                    case "smallint":
                        payload +=2;
                        break;
                    case "int":
                        payload +=4;
                        break;
                    case "bigint":
                        payload+=8;
                        break;
                    case "long":
                        payload+=8;
                        break;
                    case "float":
                        payload+=4;
                        break;
                    case "double":
                        payload+=8;
                        break;
                    case "year":
                        payload++;
                        break;
                    case "time":
                        payload +=4;
                        break;
                    case "datetime":
                        payload+=8;
                        break;
                    case "date":
                        payload+=8;
                        break;
                    case "text":
                        payload+=record[i].length;
                        break;
                }

            }
            int recSize = payload + 6;
            int newRowid = DavisBaseDropTable.getRowid(tableFile,numPages) + 1;

                if(numRecs == 0||recPointer - (16 + (numRecs+1)*2) > (recSize))
                {
                    if(numRecs == 0)
                    {
                        recPointer = (short) DavisBasePrompt.pageSize;
                    }
                    tableFile.seek(numPages*DavisBasePrompt.pageSize + 16 + numRecs*2);
                    tableFile.writeShort(recPointer - recSize);
                    tableFile.seek(numPages*DavisBasePrompt.pageSize + 2);
                    tableFile.writeShort(numRecs + 1);
                    tableFile.writeShort(recPointer - recSize);
                    //write record
                    tableFile.seek(numPages*DavisBasePrompt.pageSize + recPointer - recSize);
                    tableFile.writeShort(payload);
                    tableFile.writeInt(newRowid);
                    tableFile.writeByte((byte)record.length);
                    for(int i =0; i<record.length; i++)
                    {
                        switch(record[i].type){
                            case"null":
                                tableFile.writeByte(0);
                                break;
                            case "tinyint":
                                tableFile.writeByte(1);
                                break;
                            case "smallint":
                                tableFile.writeByte(2);
                                break;
                            case "int":
                                tableFile.writeByte(3);
                                break;
                            case "bigint":
                                tableFile.writeByte(4);
                                break;
                            case "long":
                                tableFile.writeByte(4);;
                                break;
                            case "float":
                                tableFile.writeByte(5);
                                break;
                            case "double":
                                tableFile.writeByte(6);
                                break;
                            case "year":
                                tableFile.writeByte(8);
                                break;
                            case "time":
                                tableFile.writeByte(9);
                                break;
                            case "datetime":
                                tableFile.writeByte(10);
                                break;
                            case "date":
                                tableFile.writeByte(11);
                                break;
                            case "text":
                                tableFile.writeByte((12 + record[i].length));
                                //tableFile.writeByte(record[i].length);
                                break;
                        }
                    }
                    for(int i =0; i<record.length; i++)
                    {
                        switch(record[i].type){
                            case"null":
                                break;
                            case "tinyint":
                                tableFile.writeByte(Byte.parseByte(record[i].value));
                                break;
                            case "smallint":
                                tableFile.writeShort(Short.parseShort(record[i].value));
                                break;
                            case "int":
                                tableFile.writeInt(Integer.parseInt(record[i].value));
                                break;
                            case "bigint":
                                tableFile.writeLong(Long.parseLong(record[i].value));
                                break;
                            case "long":
                                tableFile.writeLong(Long.parseLong(record[i].value));
                                break;
                            case "float":
                                tableFile.writeFloat(Float.parseFloat(record[i].value));
                                break;
                            case "double":
                                tableFile.writeDouble(Double.parseDouble(record[i].value));
                                break;
                            case "year":
                                tableFile.writeByte(Byte.parseByte(record[i].value));
                                break;
                            case "time":
                                tableFile.writeInt(Integer.parseInt(record[i].value));
                                break;
                            case "datetime":
                                tableFile.writeLong(Long.parseLong(record[i].value));
                                break;
                            case "date":
                                tableFile.writeBytes(record[i].value);
                                break;
                            case "text":
                                tableFile.writeBytes(record[i].value);
                                break;
                        }
                    }
                }
                else
                {
                    tableFile.setLength(tableFile.length()+ DavisBasePrompt.pageSize);
                    numPages++;
                    setupPage(numPages, tableFile);
                    tableFile.seek(numPages*DavisBasePrompt.pageSize + 2);
                    tableFile.writeShort(1);
                    tableFile.writeShort((short) (DavisBasePrompt.pageSize -recSize));
                    tableFile.seek(numPages*DavisBasePrompt.pageSize + 16);
                    tableFile.writeShort((short) (DavisBasePrompt.pageSize -recSize));
                    //write record
                    tableFile.seek(numPages*DavisBasePrompt.pageSize + DavisBasePrompt.pageSize -recSize);
                    tableFile.writeShort(payload);
                    tableFile.writeInt(newRowid);
                    tableFile.writeByte((byte)record.length);
                    for(int i =0; i<record.length; i++)
                    {
                        switch(record[i].type){
                            case"null":
                                tableFile.writeByte(0);
                                break;
                            case "tinyint":
                                tableFile.writeByte(1);
                                break;
                            case "smallint":
                                tableFile.writeByte(2);
                                break;
                            case "int":
                                tableFile.writeByte(3);
                                break;
                            case "bigint":
                                tableFile.writeByte(4);
                                break;
                            case "long":
                                tableFile.writeByte(4);;
                                break;
                            case "float":
                                tableFile.writeByte(5);
                                break;
                            case "double":
                                tableFile.writeByte(6);
                                break;
                            case "year":
                                tableFile.writeByte(8);
                                break;
                            case "time":
                                tableFile.writeByte(9);
                                break;
                            case "datetime":
                                tableFile.writeByte(10);
                                break;
                            case "date":
                                tableFile.writeByte(11);
                                break;
                            case "text":
                                tableFile.writeByte((12 + record[i].length));
                                //tableFile.writeByte(record[i].length);
                                break;
                        }
                    }
                    for(int i =0; i<record.length; i++)
                    {
                        switch(record[i].type){
                            case"null":
                                break;
                            case "tinyint":
                                tableFile.writeByte(Byte.parseByte(record[i].value));
                                break;
                            case "smallint":
                                tableFile.writeShort(Short.parseShort(record[i].value));
                                break;
                            case "int":
                                tableFile.writeInt(Integer.parseInt(record[i].value));
                                break;
                            case "bigint":
                                tableFile.writeLong(Long.parseLong(record[i].value));
                                break;
                            case "long":
                                tableFile.writeLong(Long.parseLong(record[i].value));
                                break;
                            case "float":
                                tableFile.writeFloat(Float.parseFloat(record[i].value));
                                break;
                            case "double":
                                tableFile.writeDouble(Double.parseDouble(record[i].value));
                                break;
                            case "year":
                                tableFile.writeByte(Byte.parseByte(record[i].value));
                                break;
                            case "time":
                                tableFile.writeInt(Integer.parseInt(record[i].value));
                                break;
                            case "datetime":
                                tableFile.writeLong(Long.parseLong(record[i].value));
                                break;
                            case "date":
                                tableFile.writeBytes(record[i].value);
                                break;
                            case "text":
                                tableFile.writeBytes(record[i].value);
                                break;
                        }
                    }
                }
            System.out.println("Record(s) Updated!!");
            tableFile.close();
        }
        catch(IOException e)
        {

        }
    }


}

class Update
{
    String value;
    String type;
    int length;
    int ordinal;
    Update(String type, String value, byte ordinal)
    {
        this.type = type;
        this.value = value;
        this.ordinal = ordinal & 0xFF;
    }
    Update(String type, int length)
    {
        this.type = type;
        this.length = length;
    }



}



class Column
{
    String table;
    String name;
    String type;
    String isnull;
    byte ordinal;
    Column(String table,String name, String type, String isnull, int ordinal)
    {
        this.table = table;
        this.name = name;
        this.type = type;
        this.isnull = isnull;
        this.ordinal = (byte)ordinal ;
    }

}
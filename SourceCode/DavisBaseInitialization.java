import static java.lang.System.out;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DavisBaseInitialization {

	/* This static variable controls page size. */
	static int pageSizePower = 9;
	/* This strategy insures that the page size is always a power of 2. */
	static int pageSize = (int)Math.pow(2, pageSizePower);
	static int leafOrNode 			= 6;

	/**
	 * This static method creates the DavisBase data storage container
	 * and then initializes two .tbl files to implement the two 
	 * system tables, davisbase_tables and davisbase_columns
	 *
	 *  WARNING! Calling this method will destroy the system database
	 *           catalog files if they already exist.
	 */
	public static void initializeDataStore() {

		/** Create data directory at the current OS location to hold */
		
		Path dir = Paths.get("data");
        Path table = Paths.get("data/davisbase_tables.tbl");
        Path columns = Paths.get("data/davisbase_columns.tbl");
        if(!Files.isDirectory(dir) || !Files.exists(table) || !Files.exists(columns))
        {
            System.out.println("Database doesn't exist!!");
        	System.out.println("Inititalizing database....");
        	try {
        		File dataDir = new File("data");
        		dataDir.mkdir();
        		String[] oldTableFiles;
        		oldTableFiles = dataDir.list();
        		for (int i=0; i<oldTableFiles.length; i++) {
        			File anOldFile = new File(dataDir, oldTableFiles[i]);
        			/**
        			 * 	Delete any existing files in 'data' directory
        			 * Note: Delete may fail if the previous RandomAccessFile object is still holding the file
        			 *       In such cases delete the files manually in the directory
        			 */
        			if (anOldFile.delete()) {
        				//System.out.println(oldTableFiles[i] + " is deleted");
        			} else {
        				//System.out.println(oldTableFiles[i] + " was not deleted");
        			}
        		}
        	} catch (SecurityException se) {
        		out.println("Unable to create data container directory");
        		out.println(se);
        	}
        	
        	/** Create davisbase_tables system catalog */
        	try {
        		RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
        		/* Initially, the file is one page in length */
        		davisbaseTablesCatalog.setLength(pageSize);
        		/* Set file pointer to the beginning of the file */
        		davisbaseTablesCatalog.seek(0);
        		/* Write 0x0D to the page header to indicate that it's a leaf page.  
        		 *The file pointer will automatically increment to the next byte. */
        		davisbaseTablesCatalog.writeByte(0x0D);	// First byte of page
        		/* Write 0x00 (although its value is already 0x00) to indicate there 
        		 * are no cells on this page */
        		/**
        		 * First 16 bytes of every page in a file would be the page Header
        		 * Followed the file format guide lines provided in 
        		 * "DavisBase Nano File Format Guide (SDL).pdf" Section 3.1 Page Headers
        		 */
        		davisbaseTablesCatalog.seek(leafOrNode);
        		/** 
        		 *	Bytes 7 to 10 represent the page number of right sibling if current page is leaf
        		 * and page number of rightmost child if current page is interior/root node.
        		 * If no right sibling or rightmost child, then  the special value 0xFFFFFFFF is used.
        		 */
        		davisbaseTablesCatalog.writeShort(65535);
        		davisbaseTablesCatalog.writeShort(65535);
        		/** 
        		 * Bytes 11 to 14 represent the parent page in the b+ tree.
        		 * If root, then the special value 0xFFFFFFFF is used.
        		 */
        		davisbaseTablesCatalog.writeShort(65535);
        		davisbaseTablesCatalog.writeShort(65535);
        		/**
        		 * Failed to close the file causes RandomAccessFile object to associate with 
        		 * file even after program ends causing file unable to be deleted. 
        		 */
        		davisbaseTablesCatalog.close();
        	} catch (Exception e) {
        		out.println("Unable to create the database_tables file");
        		out.println(e);
        	}

        	/** Create davisbase_columns systems catalog */
        	try {
        		RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
        		/** Initially the file is one page in length */
        		davisbaseColumnsCatalog.setLength(pageSize);
        		davisbaseColumnsCatalog.seek(0);       // Set file pointer to the beginning of the file
        		/* Write 0x0D to the page header to indicate a leaf page. The file 
        		 * pointer will automatically increment to the next byte. */
        		davisbaseColumnsCatalog.writeByte(0x0D);
        		/* Write 0x00 (although its value is already 0x00) to indicate there 
        		 * are no cells on this page */
        		davisbaseColumnsCatalog.seek(leafOrNode);
        		davisbaseColumnsCatalog.writeShort(65535);
        		davisbaseColumnsCatalog.writeShort(65535);
        		davisbaseColumnsCatalog.writeShort(65535);
        		davisbaseColumnsCatalog.writeShort(65535);
        		davisbaseColumnsCatalog.close();
        		System.out.println("Database Initialized!!");
        	} catch (Exception e) {
        		out.println("Unable to create the database_columns file");
        		out.println(e);
        	}
        }
	}   
}
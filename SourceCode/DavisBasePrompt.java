import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import static java.lang.System.out;

public class DavisBasePrompt {

	static String prompt = "davisql> ";
	static String version = "Team Red";
	static String copyright = "©2016 Chris Irwin Davis";
	static boolean isExit = false;
	/*
	 * Page size for all files is 512 bytes by default.
	 * You may choose to make it user modifiable
	 */
	static long pageSize = 512; 

	/* 
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	/** ***********************************************************************
	 *  Main method
	 */
    public static void main(String[] args) {

		/* Display the welcome screen */
		splashScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = ""; 
		DavisBaseInitialization.initializeDataStore();
		
		while(!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");
	}

	/** ***********************************************************************
	 *  Static method definitions
	 */

	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}
	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}
	
	/**
	 *  Help: Display supported commands
	 */
	public static void help() {
		out.println(line("*",80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		//printCmd("SELECT * FROM <table_name>;");
		//printDef("Display all records in the table <table_name>.");
		out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
		out.println("\tDisplay table records whose optional <condition>");
		out.println("\tis <column_name> = <value>.\n");
		out.println("DROP TABLE <table_name>;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition> is\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(line("*",80));
	}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}
		
	public static void parseUserCommand (String userCommand) {
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		/*
		*  This switch handles a very small list of hard coded commands of known syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		switch (commandTokens.get(0)) {
			case "create":
				//System.out.println("CASE: CREATE");
				DavisBaseCreateTable.parseCreateTable(userCommand);
				break;
			case "drop":
				//System.out.println("CASE: DROP");
				dropTable(userCommand);
				break;
			case "show":
				//System.out.println("CASE: SHOW");
				DavisBaseShowTables.parseShow(userCommand);
				break;
			case "insert":
				//System.out.println("CASE: INSERT");
				DavisBaseInsert.parseInsert(userCommand);
				break;
			case "delete":
				//System.out.println("CASE: DELETE");
				DavisBaseDeleteRecord.deleteRecord(userCommand);
				break;
			case "update":
				//System.out.println("CASE: UPDATE");
				DavisBaseUpdateRecord.updateRecord(userCommand);
				break;
			case "select":
				//System.out.println("CASE: SELECT");
				DavisBaseSelect.parseQuery(userCommand);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
			case "quit":
				isExit = true;
				break;
			case "hexdump":
				DavisBaseHexDump.parseHexDump(userCommand);
				break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}

	/**
	 *  Stub method for dropping tables
	 *  @param dropTableString is a String of the user input
	 */
	public static void dropTable(String dropTableString) {
		ArrayList<String> cmdTokens = new ArrayList<String>(Arrays.asList(dropTableString.split("\s+")));
		DavisBaseUtils.trimArrayFields(cmdTokens);

		if (cmdTokens.size() != 3 || (!cmdTokens.get(1).equals("table"))) {
			System.out.println("Invalid drop command");
			System.out.println("USAGE:    drop table <table_name>;");
		} else {
			String tableName = cmdTokens.get(2);
			DavisBaseDropTable.dropTable(tableName);
		}
	}
}
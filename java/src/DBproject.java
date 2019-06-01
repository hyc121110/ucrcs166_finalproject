/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Plane");
				System.out.println("2. Add Pilot");
				System.out.println("3. Add Flight");
				System.out.println("4. Add Technician");
				System.out.println("5. Book Flight");
				System.out.println("6. List number of available seats for a given flight.");
				System.out.println("7. List total number of repairs per plane in descending order");
				System.out.println("8. List total number of repairs per year in ascending order");
				System.out.println("9. Find total number of passengers with a given status");
				System.out.println("10. < EXIT");
				
				switch (readChoice()){
					case 1: AddPlane(esql); break;
					case 2: AddPilot(esql); break;
					case 3: AddFlight(esql); break;
					case 4: AddTechnician(esql); break;
					case 5: BookFlight(esql); break;
					case 6: ListNumberOfAvailableSeats(esql); break;
					case 7: ListsTotalNumberOfRepairsPerPlane(esql); break;
					case 8: ListTotalNumberOfRepairsPerYear(esql); break;
					case 9: FindPassengersCountWithStatus(esql); break;
					case 10: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static void AddPlane(DBproject esql) {//1
    // Plane attributes
    String make;
    String model;
    int age;
    int seats;

    // make (0-32)
    do {
      System.out.print("\tPlease input the make of the plane: ");
      try {
        make = in.readLine(); // prompt input from user
        // check length of input is 0 < input.length < 32
        if (make.length() <= 0 || make.length() > 32) {
          // throw exception
          throw new RuntimeException("Number of characters must be between 1 to 32 characters!");
        }
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    // model (0-64)
    do {
      System.out.print("\tPlease input the model of the plane: ");
      try {
        model = in.readLine(); // prompt input from user
        // check length of input is 0 < input.length < 64
        if (model.length() <= 0 || model.length() > 64) {
          // throw exception
          throw new RuntimeException("Number of characters must be between 1 to 64 characters!");
        }
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    // age
    do {
      System.out.print("\tPlease input the age of the plane: ");
      try {
        age = Integer.parseInt(in.readLine()); // prompt input from user
        // check length of input is 0 < input.length < 64
        if (age < 0) {
          // throw exception
          throw new RuntimeException("Plane Age must be at least 0!");
        }
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    // seats (0 - 499)
    do {
      System.out.print("\tPlease input the number of seats of the plane: ");
      try {
        seats = Integer.parseInt(in.readLine()); // prompt input from user
        // check length of input is 0 < input.length < 64
        if (seats <= 0 || seats > 499) {
          // throw exception
          throw new RuntimeException("Number of seats must be between 1 to 499!");
        }
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    // insert all attributes intoplane table
    try {
      String query = "INSERT INTO Plane(make, model, age, seats) VALUES (" + "\'" + make + "\', \'" + model + "\', " + age + ", " + seats + ");";
      esql.executeUpdate(query);
    } catch (Exception e) {
      System.err.println (e.getMessage()); // lab6
    }
    
    try {
      String query = "SELECT * FROM PLANE";
      esql.executeQueryAndPrintResult(query);
    } catch (Exception e) {
      System.err.println (e.getMessage()); // lab6
    }
	}

	public static void AddPilot(DBproject esql) {//2
    // pilot attributes
    String fullname;
    String nationality;
    
    // fullname
    do {
      System.out.print("\tPlease input the full name of the pilot: ");
      try {
        fullname = in.readLine(); // prompt input from user
        // check length of input is 0 < input.length < 128
        if (fullname.length() <= 0 || fullname.length() > 128) {
          // throw exception
          throw new RuntimeException("Number of characters must be between 1 to 128!");
        }
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    // nationality
    do {
      System.out.print("\tPlease input the nationality of the pilot: ");
      try {
        nationality = in.readLine(); // prompt input from user
        // check length of input is 0 < input.length < 24
        if (nationality.length() <= 0 || nationality.length() > 24) {
          // throw exception
          throw new RuntimeException("Number of characters must be between 1 to 24!");
        }
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    // insert all attributes into pilot table
    try {
      String query = "INSERT INTO Pilot(fullname, nationality) VALUES (\'" + fullname + "\', \'" + nationality + "\');";
	  esql.executeUpdate(query);
    } catch (Exception e) {
      System.err.println (e.getMessage()); // lab6
    }
    
    try {
      String query = "SELECT * FROM Pilot";
      esql.executeQueryAndPrintResult(query);
    } catch (Exception e) {
      System.err.println (e.getMessage()); // lab6
    }
	}

	public static void AddFlight(DBproject esql) {//3
		// Given a pilot, plane and flight, adds a flight in the DB
		//FI: flightid, pilotid, planeid, all int
		//flight: cost,num_sold,num_stops,act_dept_date,act_arr_date,arrive airport, dept airpot
		int fid; //flightID
		int pid; //pilotID
		int Pid; //planeID
    int cost;
    int sold;
    int stops;
    String dDate;
    String aDate;
    String arr_airport;
    String dept_airport;
	  do {
      System.out.print("\tPlease input the flight id of the plane: ");
      try {
        fid = Integer.parseInt(in.readLine()); // prompt input from user
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    do {
      System.out.print("\tPlease input the pilot id of the plane: ");
      try {
        pid = Integer.parseInt(in.readLine()); // prompt input from user
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    do {
      System.out.print("\tPlease input the plane id of the plane: ");
      try {
        Pid = Integer.parseInt(in.readLine()); // prompt input from user
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
     try {
      String query = "INSERT INTO FlightInfo(flight_id, pilot_id, plane_id) VALUES (" + fid + ", " + pid + ", " + Pid + ");";
      esql.executeUpdate(query);
    } catch (Exception e) {
      System.err.println (e.getMessage());
    }
    
      
    
    do {
      System.out.print("\tPlease input the flight cost: ");
      try {
        cost = Integer.parseInt(in.readLine()); // prompt input from user
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    do {
      System.out.print("\tPlease input the number of seats sold: ");
      try {
        sold = Integer.parseInt(in.readLine()); // prompt input from user
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    do {
      System.out.print("\tPlease input the number of stops: ");
      try {
        stops = Integer.parseInt(in.readLine()); // prompt input from user
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    do {
      System.out.print("\tPlease input the actual departure date: ");
      try {
        dDate = in.readLine(); // prompt input from user
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    do {
      System.out.print("\tPlease input the actual arrival date: ");
      try {
        aDate = in.readLine(); // prompt input from user
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    do {
      System.out.print("\tPlease input the actual arrival airport: ");
      try {
        arr_airport = in.readLine(); // prompt input from user
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    do {
      System.out.print("\tPlease input the actual departure airport: ");
      try {
        dept_airport = in.readLine(); // prompt input from user
        break;
      } catch (Exception e) {
        System.out.println("Invalid Input!");
        continue;
      }
    } while (true);
    
    try {
      String query = "INSERT INTO Flight(cost, num_sold, num_stops, actual_departure_date, actual_arrival_date, arrival_airport, departure_airport) VALUES (" + cost + ", " + sold + ", " + stops + ", \'" + dDate + "\', \'" + aDate + "\', \'" + arr_airport + "\', \'" + dept_airport + "\');";
      esql.executeUpdate(query);
    } catch (Exception e) {
      System.err.println (e.getMessage());
    }
  		
	}
	
	public static void AddTechnician(DBproject esql) {//4
    // technician attributes
    String full_name;
    
    // full_name
    do {
      System.out.print("\tPlease input the full name of the technician: ");
      try {
        full_name = in.readLine(); // prompt input from user
        // check length of input is 0 < input.length < 128
        if (full_name.length() <= 0 || full_name.length() > 128) {
          // throw exception
          throw new RuntimeException("Number of characters must be between 1 to 128!");
        }
        break;
      } catch (Exception e) {
        System.out.println("Invalid input!");
        continue;
      }
    } while (true);
    
    // execute query
    try {
      String query = "INSERT INTO Technician(full_name) VALUES (\'" + full_name + "\');";
      esql.executeUpdate(query);
    } catch (Exception e) {
      System.err.println (e.getMessage()); // lab6
    }
    
    try {
      String query = "SELECT * FROM Technician";
      esql.executeQueryAndPrintResult(query);
    } catch (Exception e) {
      System.err.println (e.getMessage()); // lab6
    }
	}

	public static void BookFlight(DBproject esql) {//5
		// Given a customer and a flight that he/she wants to book, add a reservation to the DB
    
    // get customer id
    int cus_id;
    do {
      System.out.print("\tPlease input the customer ID: ");
      try {
        cus_id = Integer.parseInt(in.readLine()); // convert string to int
        break;
      } catch (Exception e) {
        System.out.println("Invalid input!");
        continue;
      }
    } while (true);
    
    // get flight id
    int fid;
    do {
      System.out.print("\tPlease input the flight ID: ");
      try {
        fid = Integer.parseInt(in.readLine()); // convert string to int
        break;
      } catch (Exception e) {
        System.out.println("Invalid input!");
        continue;
      }
    } while (true);
    
    // query the number of available seats
    try {
      // gets the number of seats sold
			String query1 = "SELECT F.num_sold " + 
                     "FROM Flight F " + 
                     "WHERE F.fnum = " + fid + ";";
      
      // total number of seats on the plane
      String query2 = "SELECT P.seats " + 
                     "FROM Plane P, FlightInfo FI, Flight F " + 
                     "WHERE F.fnum = " + fid + " AND FI.flight_id = F.fnum AND P.id = FI.plane_id;";
      // available seats
      int availableSeats = Integer.parseInt(query2) - Integer.parseInt(query1);
      
      String status;
      if (availableSeats > 0) {
        status = "C";
      } else {
        status = "W";
      }
      // rnum, cid, fid, status
      // somehow need to append to end of reservation list (todo)
      String query = "INSERT INTO Reservation(cid, fid, status) VALUES (" + cus_id + ", " + fid + ", \'" + status + "\');";
			esql.executeUpdate(query);
      System.out.println("Successfully booked flight!");
		} catch (Exception e) {
			System.err.println (e.getMessage());
		}
	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//6
		// For flight number and date, find the number of availalbe seats (i.e. total plane capacity minus booked seats )
    int fid;
    do {
      System.out.print("\tPlease input the flight ID: ");
      try {
        fid = Integer.parseInt(in.readLine()); // convert string to int
        break;
      } catch (Exception e) {
        System.out.println("Invalid input!");
        continue;
      }
    } while (true);
     try {
			String query = "SELECT (T.seat - T1.seat) AS available FROM (SELECT P.seats AS seat "
                      + "FROM Plane P,FlightInfo FI, Flight F1 " +
                      "WHERE F1.fnum = " + fid + " AND FI.flight_id = F1.fnum AND P.id = FI.plane_id) AS T,"
                      + "(SELECT F2.num_sold as seat FROM Flight F2" +  " WHERE F2.fnum = " + fid + ") AS T1; ";
			esql.executeQueryAndPrintResult(query); 
		} catch (Exception e) {
			System.err.println (e.getMessage());
		}
	}

	public static void ListsTotalNumberOfRepairsPerPlane(DBproject esql) {//7
		// Count number of repairs per planes and list them in descending order
    try {
			String query = "SELECT R.plane_id, COUNT(R.rid)" + 
                      "FROM Repairs R GROUP BY plane_id " +
                    "ORDER BY count DESC;";
			
			esql.executeQueryAndPrintResult(query); 
		} catch (Exception e) {
			System.err.println (e.getMessage());
		}
	}

	public static void ListTotalNumberOfRepairsPerYear(DBproject esql) {//8
		// Count repairs per year and list them in ascending order
    try {
			String query = "SELECT EXTRACT (year FROM R.repair_date) as \"Year\", count(R.rid) " + // keyword EXTRACT to extract from certain year
                     "FROM Repairs R " + 
                     "GROUP BY \"Year\" " + // group repairs by year
                     "ORDER BY count ASC;"; // list count in ascending order
			
			esql.executeQueryAndPrintResult(query); 
		} catch (Exception e) {
			System.err.println (e.getMessage());
		}
	}
	
	public static void FindPassengersCountWithStatus(DBproject esql) {//9
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number.
    
    // get fid from Reservation
    int fid;
    do {
			System.out.print("\tPlease enter a flight Number: ");
			try {
				fid = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Invalid input!");
				continue;
			}
		} while (true);
    
    // get status from Reservation
    // if status not equals to "W", "C" or "R", throw new exception
    String status;
    do {
			System.out.print("\tPlease enter a customer status: ");
			try {
				status = in.readLine();
        if (!status.equals("W") && !status.equals("C") && !status.equals("R")) {
          throw new RuntimeException("Please enter a valid status (W, C, R)!");
        }
				break;
			} catch (Exception e) {
				System.out.println("Invalid Input!");
				continue;
			}
		} while (true);
    
    // process the query   
    try {
			String query = "SELECT COUNT(*) " + 
                     "FROM Reservation R " + 
                     "WHERE fid = " + fid + " AND status = \'" + status + "\';"; // fid + status
			
			esql.executeQueryAndPrintResult(query);
		} catch (Exception e) {
			System.err.println (e.getMessage());
		}
	}
}

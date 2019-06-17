import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import javax.management.Query;

/**
 * Runs queries against a back-end database.
 * This class is responsible for searching for flights.
 */
public class QuerySearchOnly
{
  // `dbconn.properties` config file
  private String configFilename;

  // DB Connection
  protected Connection conn;
  
  // Canned queries
  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  protected PreparedStatement checkFlightCapacityStatement;
  
  //search direct light
  private static final String SEARCH_DIRECT_FLIGHT ="SELECT TOP (?) * FROM Flights WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? AND canceled <> 1 ORDER BY actual_time ASC, fid ASC;";
  protected PreparedStatement directFlightStatement;
  // Search indirect light
  private static final String SEARCH_INDIRECT_FLIGHT = "SELECT TOP (?) * FROM Flights f1, Flights f2 WHERE f1.origin_city = ? AND f1.dest_city = f2.origin_city AND f2.dest_city = ? AND f1.day_of_month = ? AND f1.day_of_month = f2.day_of_month AND f1.canceled <> 1 AND f2.canceled <> 1 ORDER BY (f2.actual_time + f1.actual_time) ASC, f1.fid ASC;";
  protected PreparedStatement indirectFlightStatement;
  //transactions
  private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
  protected PreparedStatement beginTransactionStatement;

  private static final String COMMIT_SQL = "COMMIT TRANSACTION";
  protected PreparedStatement commitTransactionStatement;

  private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
  protected PreparedStatement rollbackTransactionStatement;
  // Itinerary
  class itinerary{
	 Flight firstL;
	 Flight secondL;
	 int dayofmonth;
	 int price;
  }
  ArrayList<itinerary> itineraries;
  StringBuffer dataR = new StringBuffer();
  
  class Flight
  {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    @Override
    public String toString()
    {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price;
    }
  }

  public QuerySearchOnly(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /** Open a connection to SQL Server in Microsoft Azure.  */
  public void openConnection() throws Exception
  {
    Properties configProps = new Properties();
    configProps.load(new FileInputStream(configFilename));

    String jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    String jSQLUrl = configProps.getProperty("flightservice.url");
    String jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    String jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");
    
    //conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    /* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

    /* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement
    /* In the full Query class, you will also want to appropriately set the transaction's isolation level:
          conn.setTransactionIsolation(...)
       See Connection class's JavaDoc for details.
    */
    
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
    /* add here more prepare statements for all the other queries you need */
    /* . . . . . . */
    beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
	commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
	rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);
	
    indirectFlightStatement = conn.prepareStatement(SEARCH_INDIRECT_FLIGHT);
    directFlightStatement = conn.prepareStatement(SEARCH_DIRECT_FLIGHT);
  }



  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise it searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries)
  {
    // Please implement your own (safe) version that uses prepared statements rather than string concatenation.
    // You may use the `Flight` class (defined above).
	
	itineraries = new ArrayList<itinerary>();
	try {
		int k = numberOfItineraries;
		// load direct fight
		if((k > 0 && !directFlight)|| k==1) {
		directFlightStatement.clearParameters();
		directFlightStatement.setInt(1, numberOfItineraries);
		directFlightStatement.setString(2, originCity);
		directFlightStatement.setString(3, destinationCity);
		directFlightStatement.setInt(4, dayOfMonth);
		
		ResultSet oneHopResults = directFlightStatement.executeQuery();
		while((numberOfItineraries> 0) && oneHopResults.next()) {
			Flight uniqF = fPush(oneHopResults, true);
			
			// save itinerary
			itinerary iner = new itinerary();
			iner.firstL = uniqF;
			iner.secondL = null;
			iner.dayofmonth = dayOfMonth;
			iner.price = uniqF.price;
			itineraries.add(iner);
			
			dataR.append("Itinerary " +(numberOfItineraries - k)+": 1 flight(s), " +uniqF.time+ " minutes\n");
			dataR.append(uniqF.toString() + "\n");
			k--;
		}
		oneHopResults.close();
		}
		
		// not direct
		if(k > 0 && !directFlight) {
			indirectFlightStatement.clearParameters();
			indirectFlightStatement.setInt(1, numberOfItineraries);
			indirectFlightStatement.setString(2, originCity);
			indirectFlightStatement.setString(3, destinationCity);
			indirectFlightStatement.setInt(4, dayOfMonth);
			
			ResultSet secondHop = indirectFlightStatement.executeQuery();
			while ( (k > 0) && secondHop.next()) {
				Flight firstF = fPush(secondHop, true);
				Flight secondF = fPush(secondHop, false);
				
				// save itinerary
				itinerary iner1 = new itinerary();
				iner1.firstL = firstF;
				iner1.secondL = secondF;
				iner1.dayofmonth = dayOfMonth;
				iner1.price = firstF.price + secondF.price ;
				itineraries.add(iner1);
				
				dataR.append("Itinerary " +(numberOfItineraries - k)+": 2 flight(s), " +(secondF.time+firstF.time)+ " minutes\n");
				dataR.append(firstF.toString() + "\n");
				dataR.append(secondF.toString() + "\n");
				k--;
			}
			secondHop.close();
		}
		
	} catch (Exception e) {
		e.printStackTrace();
	}
	if(dataR.toString().trim().equalsIgnoreCase("")) {
		return "No flights match your selection\n";
	}
    return dataR.toString();
    
  }
  
  protected Flight fPush (ResultSet re, boolean ortwo) throws SQLException {
	  int second = 0;
	  if (!ortwo) {
		  second = 18;
	  }
	  Flight valueF = new Flight();
	  valueF.fid = re.getInt(1 + second);
	  valueF.dayOfMonth = re.getInt(3 + second);
	  valueF.carrierId = re.getString(5 + second);
	  valueF.flightNum = re.getString(6 + second);
	  valueF.originCity = re.getString(7 + second);
	  valueF.destCity = re.getString(9 + second);
	  valueF.time = re.getInt(15 + second);
	  valueF.capacity = re.getInt(17 + second);
	  valueF.price = re.getInt(18 + second);
	  return valueF;
	  
  }
  /**
   * Same as {@code transaction_search} except that it only performs single hop search and
   * do it in an unsafe manner.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight
   * @param dayOfMonth
   * @param numberOfItineraries
   *
   * @return The search results. Note that this implementation *does not conform* to the format required by
   * {@code transaction_search}.
   */
  private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
                                          int dayOfMonth, int numberOfItineraries)
  {
    StringBuffer sb = new StringBuffer();

    try
    {
      // one hop itineraries
      String unsafeSearchSQL =
              "SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
                      + "FROM Flights "
                      + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " "
                      + "ORDER BY actual_time ASC";

      Statement searchStatement = conn.createStatement();
      ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

      while (oneHopResults.next())
      {
        int result_dayOfMonth = oneHopResults.getInt("day_of_month");
        String result_carrierId = oneHopResults.getString("carrier_id");
        String result_flightNum = oneHopResults.getString("flight_num");
        String result_originCity = oneHopResults.getString("origin_city");
        String result_destCity = oneHopResults.getString("dest_city");
        int result_time = oneHopResults.getInt("actual_time");
        int result_capacity = oneHopResults.getInt("capacity");
        int result_price = oneHopResults.getInt("price");

        sb.append("Day: ").append(result_dayOfMonth)
                .append(" Carrier: ").append(result_carrierId)
                .append(" Number: ").append(result_flightNum)
                .append(" Origin: ").append(result_originCity)
                .append(" Destination: ").append(result_destCity)
                .append(" Duration: ").append(result_time)
                .append(" Capacity: ").append(result_capacity)
                .append(" Price: ").append(result_price)
                .append('\n');
      }
      oneHopResults.close();
    } catch (SQLException e) { e.printStackTrace(); }

    return sb.toString();
  }

  /**
   * Shows an example of using PreparedStatements after setting arguments.
   * You don't need to use this method if you don't want to.
   */
  private int checkFlightCapacity(int fid) throws SQLException
  {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }

	/* some utility functions below */

	public void beginTransaction() throws SQLException
	{
		conn.setAutoCommit(false);
		beginTransactionStatement.executeUpdate();
	}

	public void commitTransaction() throws SQLException
	{
		commitTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}

	public void rollbackTransaction() throws SQLException
	{
		rollbackTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}

}

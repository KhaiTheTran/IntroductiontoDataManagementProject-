import java.sql.*;
import java.util.ArrayList;

import javax.swing.plaf.SliderUI;

//import Query.Itinerary;

public class Query extends QuerySearchOnly {

	// Logged In User
	private String username; // customer username is unique
	private static final String Get_User = "SELECT * FROM Users WHERE username = ? AND password = ?;";
	protected PreparedStatement getUserTransactionStatement;
	private static final String Get_User1 = "SELECT * FROM Users WHERE username = ?;";
	protected PreparedStatement getUserTran1;
	
	private static final String up_User1 = "UPDATE Users set balance = ? WHERE username = ?;";
	protected PreparedStatement upUserTran1;
	
	// create user
	private static final String Cre_User = "INSERT INTO Users VALUES (?,?,?);";
    protected PreparedStatement CreUderTran;
	
	// clear table
	private static final String cl_Users = "DELETE FROM Users;";
	protected PreparedStatement clUserTran;
	
	private static final String cl_Reservations = "DELETE FROM Reservations;";
	protected PreparedStatement clReTran;
	
	private static final String cl_Capacities = "DELETE FROM Capacities;";
	protected PreparedStatement cl_CaTran;
	
	private static final String cl_ReNume = "DELETE FROM ReNume;";
	protected PreparedStatement clRnTran;
	
	//get Reservations
	private static final String get_reservation = "SELECT * FROM Reservations WHERE username = ?;";
	protected PreparedStatement getReTran;
	private static final String get_reservation1 = "SELECT * FROM Reservations WHERE rid = ?;";
	protected PreparedStatement getReTran1;
	
	private static final String up_reservation1 = "UPDATE Reservations SET paid = ? WHERE rid = ?;";
	protected PreparedStatement upReTran1;
	// delete row
	private static final String del_reservation1 = "DELETE FROM Reservations WHERE rid = ?;";
	protected PreparedStatement deLReTran1;
	
	private static final String get_reservationMaxid = "SELECT rid FROM Reservations;";
	protected PreparedStatement getReMaxidTran;
	
	// get Capacities
	private static final String get_Capacities = "SELECT * FROM Capacities WHERE fid = ?;";
	protected PreparedStatement getcapTran;
	
	// DELETE ROW
	private static final String del_Capacities = "DELETE FROM Capacities WHERE fid = ?;";
	protected PreparedStatement deLcapTran;
	
	private static final String inser_Capacities = "INSERT INTO Capacities SELECT F.fid, F.capacity FROM FLIGHTS F WHERE F.fid = ? AND NOT EXISTS (SELECT * FROM Capacities C WHERE C.fid = F.fid);";
	protected PreparedStatement inserCapTran;
	
	// update capacities
	private static final String uP_Cap = "UPDATE Capacities SET capacity = ((SELECT capacity FROM Capacities WHERE fid = ?) - 1) WHERE fid = ?;";
	protected PreparedStatement upCapTran;
	
	// insert Reservations
	private static final String insert_Reservations = "INSERT INTO Reservations VALUES (?,?,?,?,?,?,?);";
	protected PreparedStatement insertReserTran;
	
	private static final String get_ReservationsNum = "SELECT * FROM ReNume WHERE username = ?;";
	protected PreparedStatement getRENumTran;
	
	private static final String up_REserNum = "UPDATE ReNume SET numReser = ((SELECT numReser FROM ReNume WHERE username = ?) + 1) WHERE username = ?;";
	protected PreparedStatement upRenumTran;
	
	// delete row
	private static final String del_REserNum = "DELETE FROM ReNume WHERE username = ?;";
	protected PreparedStatement deLRenumTran;
	
	// insert Reservations num
	private static final String insert_ReservationsNum = "INSERT INTO ReNume VALUES (?,?);";
	protected PreparedStatement insertReserNumTran;
	
	// get flight info
	private static final String getFlightinfo = "SELECT * FROM FLIGHTS WHERE fid = ?;";
	protected PreparedStatement getFlightTran;
	
	ArrayList<Integer> cancel  = new ArrayList<Integer>();
	public Query(String configFilename) {
		super(configFilename);
	}


	/**
	 * Clear the data in any custom tables created. Do not drop any tables and do not
	 * clear the flights table. You should clear any tables you use to store reservations
	 * and reset the next reservation ID to be 1.
	 */
	public void clearTables ()
	{
		// your code here
		try {
			beginTransaction();
			cl_CaTran.executeUpdate();
			clReTran.executeUpdate();
			clRnTran.executeUpdate();
			clUserTran.executeUpdate();
			commitTransaction();
		} catch (SQLException e) {
			e.getStackTrace();
		}
		
	}


	/**
	 * prepare all the SQL statements in this method.
	 * "preparing" a statement is almost like compiling it.
	 * Note that the parameters (with ?) are still not filled in
	 */
	@Override
	public void prepareStatements() throws Exception
	{
		super.prepareStatements();
		

		/* add here more prepare statements for all the other queries you need */
		/* . . . . . . */
		getUserTransactionStatement = conn.prepareStatement(Get_User);
		getUserTran1 = conn.prepareStatement(Get_User1);
		// create user
		CreUderTran = conn.prepareStatement(Cre_User);
		// clear table
		cl_CaTran = conn.prepareStatement(cl_Capacities);
		clUserTran = conn.prepareStatement(cl_Users);
		clRnTran = conn.prepareStatement(cl_ReNume);
		clReTran = conn.prepareStatement(cl_Reservations);
		
		upUserTran1 = conn.prepareStatement(up_User1);
		
		getReTran = conn.prepareStatement(get_reservation);
		getReTran1 = conn.prepareStatement(get_reservation1);
		
		upReTran1 = conn.prepareStatement(up_reservation1);
		
		getcapTran = conn.prepareStatement(get_Capacities);
		
		inserCapTran = conn.prepareStatement(inser_Capacities);
		
		upCapTran = conn.prepareStatement(uP_Cap);
		
		insertReserTran = conn.prepareStatement(insert_Reservations);
		
		getRENumTran = conn.prepareStatement(get_ReservationsNum);
		
		upRenumTran = conn.prepareStatement(up_REserNum);
		
		insertReserNumTran = conn.prepareStatement(insert_ReservationsNum);
		
		getReMaxidTran = conn.prepareStatement(get_reservationMaxid);
		
		getFlightTran = conn.prepareStatement(getFlightinfo);
		
		deLReTran1 = conn.prepareStatement(del_reservation1);
		deLcapTran = conn.prepareStatement(del_Capacities);
		deLRenumTran = conn.prepareStatement(del_REserNum);
	}


	/**
	 * Takes a user's username and password and attempts to log the user in.
	 *
	 * @return If someone has already logged in, then return "User already logged in\n"
	 * For all other errors, return "Login failed\n".
	 *
	 * Otherwise, return "Logged in as [username]\n".
	 */
	public String transaction_login(String username, String password)
	{
		if(this.username == username ||this.username != null ) {
			return "User already logged in\n";
		}
		try {
			beginTransaction();
			getUserTransactionStatement.clearParameters();
			getUserTransactionStatement.setString(1, username);
			getUserTransactionStatement.setString(2, password);
			ResultSet log_in = getUserTransactionStatement.executeQuery();
			if(log_in.next()) {
				this.username = username;
				log_in.close();
				commitTransaction();
				return "Logged in as "+username+"\n";
			}
			rollbackTransaction();
			log_in.close();
		} catch (SQLException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		return "Login failed\n";
	}

	/**
	 * Implement the create user function.
	 *
	 * @param username new user's username. User names are unique the system.
	 * @param password new user's password.
	 * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
	 *
	 * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
	 */
	public String transaction_createCustomer (String username, String password, int initAmount)
	{	
		if (initAmount >= 0) {
		
		try {
			beginTransaction();
			getUserTran1.clearParameters();
			getUserTran1.setString(1, username);
			ResultSet lUser1 = getUserTran1.executeQuery();
			
			if(lUser1.isBeforeFirst()) {
				rollbackTransaction();
				
				return "Failed to create user\n";
			}
			lUser1.close();
			
			CreUderTran.clearParameters();
			
			CreUderTran.setString(1, username);
			
			CreUderTran.setString(2, password);
			
			CreUderTran.setInt(3, initAmount);
			CreUderTran.executeUpdate();
			commitTransaction();
			
		} catch (SQLException e) {
			e.getStackTrace();
			return "Failed to create user\n";
		}
		}else {
			//return "Failed to create user\n";
		}
		return "Created user "+username+"\n";
		
	}

	/**
	 * Implements the book itinerary function.
	 *
	 * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
	 *
	 * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
	 * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
	 * If the user already has a reservation on the same day as the one that they are trying to book now, then return
	 * "You cannot book two flights in the same day\n".
	 * For all other errors, return "Booking failed\n".
	 *
	 * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
	 * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
	 * successful reservation is made by any user in the system.
	 */
	public String transaction_book(int itineraryId)
	{
		if(this.username == null) {
			return "Cannot book reservations, not logged in\n";
		}
		if (this.itineraries == null) {
			return "Booking failed\n";
		}
		
		if(itineraryId >= this.itineraries.size() || itineraryId < 0) {
			return "No such itinerary "+itineraryId+"\n";
		}
		itinerary inery = this.itineraries.get(itineraryId);
		
		try {
			beginTransaction();
			getReTran.clearParameters();
			getReTran.setString(1, this.username);
			ResultSet reset = getReTran.executeQuery();
			int getmonth = 0;
			while(reset.next()) {
				getmonth = reset.getInt("dayRe");
				
			if (inery.dayofmonth == getmonth) {
				rollbackTransaction();
				return "You cannot book two flights in the same day\n";
			}
			}
			reset.close();
			
			// check Capacities, we can only book when there is Capacities greater than zero
			inserCapTran.clearParameters();
			inserCapTran.setInt(1, inery.firstL.fid);
			inserCapTran.execute();
		
			getcapTran.clearParameters();
			getcapTran.setInt(1, inery.firstL.fid);
			ResultSet rsCap = getcapTran.executeQuery();
			int getCa = 0;
			
			while(rsCap.next()) {
			 getCa =rsCap.getInt("capacity");
			}
			rsCap.close();
			if(getCa == 0) {
				rollbackTransaction();
				
				return "Booking failed\n";
			}
			
			
			if(inery.secondL != null) {
				// check Capacities, we can only book when there is Capacities greater than zero
				inserCapTran.clearParameters();
				inserCapTran.setInt(1, inery.secondL.fid);
				inserCapTran.execute();
				
				getcapTran.clearParameters();
				getcapTran.setInt(1, inery.secondL.fid);
				ResultSet rsCap1 = getcapTran.executeQuery();
				getCa = 0;
				while(rsCap1.next()) {
					 getCa =rsCap1.getInt("capacity");
					}
				rsCap1.close();
				if(getCa == 0) {
					rollbackTransaction();
					return "Booking failed\n";
				}
				
			}
			
			//update first flight
			upCapTran.clearParameters();
			upCapTran.setInt(1, inery.firstL.fid);
			upCapTran.setInt(2, inery.firstL.fid);
			upCapTran.execute();
			
			if(inery.secondL != null) {
				//update second flight
				upCapTran.clearParameters();
				upCapTran.setInt(1, inery.secondL.fid);
				upCapTran.setInt(2, inery.secondL.fid);
				upCapTran.execute();
			}
			// reservation num
			getRENumTran.clearParameters();
			getRENumTran.setString(1, username);
			ResultSet reNum =getRENumTran.executeQuery();
			int reTotal =0;
			while(reNum.next()) {
			 reTotal = reNum.getInt("numReser");
			}
			
			if(reTotal == 0) {
			insertReserNumTran.clearParameters();
			insertReserNumTran.setString(1, username);
			insertReserNumTran.setInt(2, 0);
			insertReserNumTran.execute();
			}else {
				upRenumTran.clearParameters();
				upRenumTran.setString(1, username);
				upRenumTran.setString(2, username);
				upRenumTran.execute();
			}
			// get max rid
			getReMaxidTran.clearParameters();
			int remax =0;
			ResultSet getMx = getReMaxidTran.executeQuery();
			while (getMx.next()) {
			remax = getMx.getInt("rid");
			}
			getMx.close();
			insertReserTran.clearParameters();
			insertReserTran.setInt(1, remax+1);
			insertReserTran.setString(2, username);
			insertReserTran.setInt(3, 0);
			insertReserTran.setInt(4, inery.price);
			insertReserTran.setInt(5, inery.dayofmonth);
			insertReserTran.setInt(6, inery.firstL.fid);
			if(inery.secondL == null) {
			insertReserTran.setInt(7, 0);
			}else {
				insertReserTran.setInt(7, inery.secondL.fid);
			}
			insertReserTran.executeUpdate();
			commitTransaction();
			
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			if(e.getMessage().contains("deadlock")) {
				return transaction_book(itineraryId);
			}
			try {
				rollbackTransaction();
			} catch (SQLException e2) {
			 return "Booking failed\n";
			}
			e.printStackTrace();
			return "Booking failed\n";
		}
		return "Booked flight(s), reservation ID: "+1+"\n";
	}
	
	
	/**
	 * Implements the pay function.
	 *
	 * @param reservationId the reservation to pay for.
	 *
	 * @return If no user has logged in, then return "Cannot pay, not logged in\n"
	 * If the reservation is not found / not under the logged in user's name, then return
	 * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
	 * If the user does not have enough money in their account, then return
	 * "User has only [balance] in account but itinerary costs [cost]\n"
	 * For all other errors, return "Failed to pay for reservation [reservationId]\n"
	 *
	 * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
	 * where [balance] is the remaining balance in the user's account.
	 */
	public String transaction_pay (int reservationId)
	{
		if (username == null) {
			return "Cannot pay, not logged in\n";
		}
		int balance =0;
		try {
			beginTransaction();
			getReTran1.clearParameters();
			getReTran1.setInt(1, reservationId);
		    ResultSet	reresult = getReTran1.executeQuery(); 
		    int paid = 0;
		    int totalP = 0;
		    boolean a = true;
		    while(reresult.next()) {
		    	a = false;
		    	paid = reresult.getInt("paid");
		    	totalP = reresult.getInt("price");
		    }
		    if (a) {
		    	reresult.close();
		    	rollbackTransaction();
		    	//return "Failed to pay for reservation " + reservationId + "\n";
		    	return "Cannot find unpaid reservation "+reservationId+" under user: "+username+"\n";
		    }
		    else {
		    	// get total paid
			    
			    if (paid == 1) {
			    	reresult.close();
			    	rollbackTransaction();
			    	return "Cannot find unpaid reservation "+reservationId+" under user: "+username+"\n";
			    }
		    }
		    // get total price
		    
		    reresult.close();
		    // get balance
		    getUserTran1.clearParameters();
		    getUserTran1.setString(1, username);
		    ResultSet reBala = getUserTran1.executeQuery();
		    int bala = 0;
		    while(reBala.next()) {
		     bala = reBala.getInt("balance");
		    }
		    reBala.close();
			if(bala < totalP) {
				rollbackTransaction();
				return "User has only "+bala+" in account but itinerary costs "+totalP+"\n";
			}
			
				// UPADE PALANCE
				 balance = (bala - totalP);
				upUserTran1.clearParameters();
				upUserTran1.setInt(1, balance);
				upUserTran1.setString(2, username);
				upUserTran1.executeUpdate();
				// update reservation
				upReTran1.clearParameters();
				upReTran1.setInt(1, 1);
				upReTran1.setInt(2, reservationId);
				upReTran1.executeUpdate();
				commitTransaction();
				
			
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.getStackTrace();
			
			return "Failed to pay for reservation " + reservationId + "\n";
		}
		return "Paid reservation: "+reservationId+" remaining balance: "+balance+"\n";

	}

	/**
	 * Implements the reservations function.
	 *
	 * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
	 * If the user has no reservations, then return "No reservations found\n"
	 * For all other errors, return "Failed to retrieve reservations\n"
	 *
	 * Otherwise return the reservations in the following format:
	 *
	 * Reservation [reservation ID] paid: [true or false]:\n"
	 * [flight 1 under the reservation]
	 * [flight 2 under the reservation]
	 * Reservation [reservation ID] paid: [true or false]:\n"
	 * [flight 1 under the reservation]
	 * [flight 2 under the reservation]
	 * ...
	 *
	 * Each flight should be printed using the same format as in the {@code Flight} class.
	 *
	 * @see Flight#toString()
	 */
	public String transaction_reservations()
	{
		if (username == null) {
			return "Cannot view reservations, not logged in\n";
		}
		StringBuffer sb = new StringBuffer();
		try {
			beginTransaction();
			getReTran.clearParameters();
			getReTran.setString(1, username);
			ResultSet reserV = getReTran.executeQuery();
			if(reserV.isBeforeFirst()) {
				while(reserV.next()) {
					String pSta = "";
					int paidN = reserV.getInt("paid");
					int rident = reserV.getInt("rid");
					if(paidN == 1) {
						pSta = "true";
					}else {pSta = "false";}
					sb.append("Reservation "+ rident +" paid: "+ pSta +":\n");
					//sb.append(getFlightInfo(reserV.getInt("directFlightid")));
					//sb.append(getFlightInfo(reserV.getInt("indirectFlightid")));
				}
				}else {
				rollbackTransaction();
				reserV.close();
				return "No reservations found\n";
			}
			reserV.close();
			commitTransaction();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			
			try {
				rollbackTransaction();
			} catch (SQLException e2) {
				System.out.println(e2.getMessage());
			}
			e.getStackTrace();
			return "Failed to retrieve reservations\n";
		}
		String[] gbd = dataR.toString().split("\n");
		String arr = "";
		for(String a: gbd) {
			arr = a;
		}
		sb.append(arr+"\n");
		return sb.toString();
	}
	/*private String getFlightInfo(int fid) {
		System.out.println(fid+" hello");
		Flight getFli = new Flight();
		if (fid != 0) {
		try {
			beginTransaction();
			getFlightTran.clearParameters();
			getFlightTran.setInt(1, fid);
			ResultSet reFli = getFlightTran.executeQuery();
			 getFli = fPush(reFli, false);
			 reFli.close();
			commitTransaction();	
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.getStackTrace();
			return e.getMessage();
		}
		
		}else {
			return "";
		}
		return getFli.toString();
	}*/
	/**
	 * Implements the cancel operation.
	 *
	 * @param reservationId the reservation ID to cancel
	 *
	 * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
	 * For all other errors, return "Failed to cancel reservation [reservationId]"
	 *
	 * If successful, return "Canceled reservation [reservationId]"
	 *
	 * Even though a reservation has been canceled, its ID should not be reused by the system.
	 */
	public String transaction_cancel(int reservationId)
	{
		// only implement this if you are interested in earning extra credit for the HW!
//		cancel.add(reservationId);
//		if (cancel.size() > 1) {
//			return "";
//		}
		if(username == null) {
			return "Cannot cancel reservations, not logged in\n";
		}
		try {
			beginTransaction();
			getReTran1.clearParameters();
			getReTran1.setInt(1, reservationId);
			ResultSet getRE = getReTran1.executeQuery();
			if(!getRE.next()) {
				rollbackTransaction();
				getRE.close();
				return "Failed to cancel reservation " + reservationId+"\n";
			}
			int paidSta =0;
			int price = 0;
			while(getRE.next()) {
			 paidSta = getRE.getInt("paid");
			 price = getRE.getInt("price");
			}
			getRE.close();
			getUserTran1.clearParameters();
			getUserTran1.setString(1, username);
			ResultSet rebalan = getUserTran1.executeQuery();
			int balan = 0;
			while(rebalan.next()) {
			 balan = rebalan.getInt("balance");
			}
			rebalan.close();
			if(paidSta == 1) {
				upUserTran1.clearParameters();
				upUserTran1.setInt(1, (balan+price));
				upUserTran1.setString(2, username);
				upUserTran1.executeUpdate();
			}
			deLRenumTran.clearParameters();
			deLRenumTran.setString(1, username);
			deLRenumTran.executeUpdate();
			
			deLcapTran.clearParameters();
			deLcapTran.setInt(1, reservationId);
			deLcapTran.executeUpdate();
			
			deLReTran1.clearParameters();
			deLReTran1.setInt(1, reservationId);
			deLReTran1.executeUpdate();
			commitTransaction();
			
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			if(e.getMessage().contains("deadlock")) {
				
				return transaction_cancel(reservationId);
			}
			try {
				rollbackTransaction();
			} catch (SQLException e2) {
				System.out.println(e2.getMessage());
				return "Failed to cancel reservation " + reservationId+"\n";
			}
			e.printStackTrace();
		
			return "Failed to cancel reservation " + reservationId+"\n";
		}
		return "Canceled reservation "+reservationId+"\n";
	}


}

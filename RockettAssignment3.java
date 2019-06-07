import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;
import java.util.Stack;
import java.io.FileInputStream;
import java.sql.*;

// Adam Rockett, CSCI 330, December 6th, 2018

// Assignment 3
// This program examines over 22 million data points about stocks and creates a table Performance
// that stores information about how a certain company is doing compared to companies in the same industry
// during a certain time interval.

public class RockettAssignment3 {

	static Connection conn = null;
	static Connection conn2 = null;

	public static void main(String[] args) throws Exception {
        // Get reader connection properties
        String paramsFile = "readerparams.txt";
        if (args.length >= 1) {
            paramsFile = args[0];
        }
        Properties connectprops = new Properties();
        connectprops.load(new FileInputStream(paramsFile));
        
        // Get writer connection properties
        String paramsFile2 = "writerparams.txt";
        if (args.length >= 1) {
            paramsFile2 = args[0];
        }
        Properties connectprops2 = new Properties();
        connectprops2.load(new FileInputStream(paramsFile2));

        try {
            // Get reader connection
            Class.forName("com.mysql.cj.jdbc.Driver");
            String dburl = connectprops.getProperty("dburl");
            String username = connectprops.getProperty("user");
            conn = DriverManager.getConnection(dburl, connectprops);
            System.out.printf("Database connection %s %s established.%n", dburl, username);
            
            // Get writer connection
            String dburl2 = connectprops2.getProperty("dburl");
            String username2 = connectprops2.getProperty("user");
            conn2 = DriverManager.getConnection(dburl2, connectprops2);
            System.out.printf("Database connection %s %s established.%n", dburl2, username2);
            
            deleteTable();
            createTable();

            ArrayList<String> industries = new ArrayList<String>();
            industries = getIndustries(industries);
            
            // Prints out number of industries and their names
            System.out.println(industries.size() + " industries found");
            for (int i = 0; i < industries.size(); i++) {
            	System.out.println(industries.get(i));
            }
            System.out.println();
            
            for (int i = 0; i < industries.size(); i++) {
            	String startDate = getStartDate(industries.get(i));
            	String endDate = getEndDate(industries.get(i));
            	
            	getIndustryInfo(industries.get(i), startDate, endDate);
            	System.out.println();
            }
            
            
            System.out.println("Database connection closed");
            conn.close();
        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                                    ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
        }
    }
	
	public static void getIndustryInfo(String industry, String startDate, String endDate) throws Exception {
		System.out.println("Processing " + industry);
		PreparedStatement pstmt = conn.prepareStatement(
    			"select Ticker, min(TransDate), max(TransDate), " + 
    			"       count(distinct TransDate) as TradingDays " + 
    			"  from Company natural join PriceVolume " + 
    			"  where Industry = ? " + 
    			"    and TransDate >= ? and TransDate <= ? " + 
    			"  group by Ticker " + 
    			"  having TradingDays >= 150 " + 
    			"  order by Ticker; ");
		
		pstmt.setString(1, industry);
		pstmt.setString(2, startDate);
		pstmt.setString(3, endDate);
		
		ResultSet rs = pstmt.executeQuery();
		

		int countTickers = 0;
		int tradingDays = 10000;
		String firstTicker = "";
		ArrayList<String> tickers = new ArrayList<String>();
		
		if (rs.next()) {
			firstTicker = rs.getString(1);
			tickers.add(firstTicker);
			countTickers++;
		}
		
		// Gets the start Dates for the intervals
		ArrayList<String> intervalStartDates = new ArrayList<String>();
		intervalStartDates = getIntervalStartDates(intervalStartDates, startDate, endDate, firstTicker);
		
		

		// Counts number of tickers in industry group
		while (rs.next()) {
			tradingDays = Math.min(tradingDays, Integer.parseInt(rs.getString(4)));
			tickers.add(rs.getString(1));
			countTickers++;
		}
		
		
		// Writes tickerReturn and industryReturn in Performance table
		for (int i = 0; i < intervalStartDates.size() - 1; i++) {
			tickerInfo(intervalStartDates.get(i), intervalStartDates.get(i + 1), industry, tickers);
		}
		
		if (countTickers > 0) {
			System.out.println(countTickers + " accepted tickers for " + industry 
					+ " (" + startDate + " - " + endDate + "), " + tradingDays + " common dates");
		} else {
			System.out.println("Insufficient data for " + industry + " => no analysis");
		}
		pstmt.close();
	}
	
	// Writes all necessary info to my username database in the Performance table.
	public static void writeInfoToDatabase(double[] tickerReturns, ArrayList<String> tickers, 
			String startDate, String endDate, String industry) throws Exception {
		double totalReturn = 0;
		for (int i = 0; i < tickerReturns.length; i++) {
			totalReturn += tickerReturns[i];
		}
		
		for (int i = 0; i < tickers.size(); i++) {
			PreparedStatement pstmt = conn2.prepareStatement(
					"insert into Performance " + 
					"	values(?, ?, ?, ?, ?, ?)");
			pstmt.setString(1, industry);
			pstmt.setString(2, tickers.get(i));
			pstmt.setString(3, startDate);
			pstmt.setString(4, endDate);
			pstmt.setString(5, String.format("%10.7f", tickerReturns[i]));
			
			double industryReturn = (totalReturn - tickerReturns[i]) / (tickers.size() - 1);
			pstmt.setString(6, String.format("%10.7f", industryReturn));
			pstmt.execute();
		}
	}
	
	// Gets the tickerReturns for each ticker in a certain industry group within the specified time interval
	// Returns an array of all of the tickerReturns
	public static void tickerInfo(String startDate, String endDate, String industry, ArrayList<String> tickers) throws Exception{
		double[] tickerReturns = new double[tickers.size()];
		String finalDate = "";
		for (int i = 0; i < tickers.size(); i++) {
			PreparedStatement pstmt = conn.prepareStatement(
					"select ticker, OpenPrice, ClosePrice, TransDate " + 
					"from Company natural join PriceVolume " + 
					"where Industry = ? and TransDate >= ? " + 
					"	and TransDate < ? and ticker = ?"
					+ " order by TransDate desc;");
			pstmt.setString(1, industry);
			pstmt.setString(2, startDate);
			pstmt.setString(3, endDate);
			pstmt.setString(4, tickers.get(i));
			
			ResultSet rs = pstmt.executeQuery();
			
			double divider = 1.0;
	    	double nextDayOpen = -1;
	    	double finalOpen = 0.0;
	    	
	    	while (rs.next()) {
	    		double openPrice = Double.parseDouble(rs.getString(2));
	    		double closePrice = Double.parseDouble(rs.getString(3));
	    		if (nextDayOpen > 0) {
	    			if (Math.abs(closePrice / nextDayOpen - 2.0) < 0.2) {
	    				divider *= 2;
	    			} else if(Math.abs(closePrice / nextDayOpen - 3.0) < 0.3) {
	    				divider *= 3;
	    			} else if(Math.abs(closePrice / nextDayOpen - 1.5) < 0.15) {
	    				divider *= 1.5;
	    			}
	    		}
	    		nextDayOpen = openPrice;
	    		finalOpen = openPrice;
	    	}
			rs.beforeFirst();
			rs.next();
			tickerReturns[i] = (Double.parseDouble(rs.getString(3)) / (finalOpen / divider)) - 1;
			finalDate = rs.getString(4);
	    	
			pstmt.close();
		}
		writeInfoToDatabase(tickerReturns, tickers, startDate, finalDate, industry);
	}
	
	// Gets all of the start dates for the intervals from the first alphabetic ticker in each industry group
	// Returns an ArrayList will the start dates.
	public static ArrayList<String> getIntervalStartDates(ArrayList<String> intervalStartDates, 
			String startDate, String endDate, String firstTicker) throws Exception {
		PreparedStatement pstmt = conn.prepareStatement(
				"select Ticker, TransDate from Company natural join PriceVolume "
				+ "where Ticker = ? and TransDate >= ? and TransDate <= ?;");
		pstmt.setString(1, firstTicker);
		pstmt.setString(2, startDate);
		pstmt.setString(3, endDate);
		
		ResultSet rs = pstmt.executeQuery();
		
		while (rs.next()) {
			intervalStartDates.add(rs.getString(2));
			for(int i = 0; i < 59; i++) {
				if (!rs.next()) {
					break;
				}
			}
		}
		pstmt.close();
		return intervalStartDates;
	}
	
	// Gets the min(max(TransDate)), which is the common end date among all of the tickers in the same industry
	public static String getEndDate(String industry) throws Exception {
		PreparedStatement pstmt = conn.prepareStatement(
    			"select min(EndDate) " + 
    			"from (select Ticker, count(distinct TransDate) as TradingDays, max(TransDate) as EndDate" + 
    			"		from PriceVolume natural join Company" + 
    			"        where Industry = ?" + 
    			"        group by Ticker" + 
    			"        having TradingDays >= 150) as T;");
    	
    	pstmt.setString(1, industry);
    	ResultSet rs = pstmt.executeQuery();
    	
    	String endDate = "";
    	if (rs.next()) {
    		endDate = rs.getString(1);
    	} else {
    		System.out.println("Error");
    	}
    	
    	pstmt.close();
    	return endDate;
	}
	
	// Gets the max(min(TransDate)), which is the common starting date among all of the tickers in the same industry
	public static String getStartDate(String industry) throws Exception {
		PreparedStatement pstmt = conn.prepareStatement(
    			"select max(StartDate) " + 
    			"from (select Ticker, count(distinct TransDate) as TradingDays, min(TransDate) as StartDate" + 
    			"		from PriceVolume natural join Company" + 
    			"        where Industry = ?" + 
    			"        group by Ticker" + 
    			"        having TradingDays >= 150) as T;");
    	
    	pstmt.setString(1, industry);
    	ResultSet rs = pstmt.executeQuery();
    	
    	String startDate = "";
    	if (rs.next()) {
    		startDate = rs.getString(1);
    	} else {
    		System.out.println("Error");
    	}
    	
    	pstmt.close();
    	return startDate;
	}
	
	// Deletes the Table "Performance" from the database if it exists
	public static void deleteTable() throws Exception {
		Statement stmt = conn2.createStatement();
		stmt.execute("drop table if exists Performance;");
		stmt.close();
	}
	
	// Creates table "Performance" with the specified attributes
	public static void createTable() throws Exception {
		Statement stmt = conn2.createStatement();
		stmt.execute("CREATE TABLE `Performance`"
				+ "("
				+ "`Industry` CHAR(30) NOT NULL,"
				+ "`Ticker` CHAR(6),"
				+ "`StartDate` CHAR(10),"
				+ "`EndDate` CHAR(10),"
				+ "`TickerReturn` CHAR(12),"
				+ "`IndustryReturn` CHAR(12)"
				+ ");");
		stmt.close();
	}
    
	// Gets the names of all of the industries present in the database
	// Returns an ArrayList with all of the industry names in it
	// Takes an ArrayList as a parameter.
    public static ArrayList<String> getIndustries(ArrayList<String> industries) throws Exception {
    	// Create and execute a query
        Statement stmt = conn.createStatement();
        ResultSet results = stmt.executeQuery("select distinct Industry from Company order by Industry");
        
        while (results.next()) {
        	industries.add(results.getString("Industry"));
        }
        stmt.close();
    	return industries;
    }
}

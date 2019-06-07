import java.util.Properties;
import java.util.Scanner;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class Demo {

    static Connection conn = null;

    public static void main(String[] args) throws Exception {
        // Get connection properties
        String paramsFile = "ConnectionParameters.txt";
        if (args.length >= 1) {
            paramsFile = args[0];
        }
        Properties connectprops = new Properties();
        connectprops.load(new FileInputStream(paramsFile));
        connectprops.setProperty("useSSL", "false");

        try {
            // Get connection
            Class.forName("com.mysql.jdbc.Driver");
            String dburl = connectprops.getProperty("dburl");
            String username = connectprops.getProperty("user");
            conn = DriverManager.getConnection(dburl, connectprops);
            System.out.printf("Database connection %s %s established.%n", dburl, username);

            showCompanies();

            // Enter Ticker and TransDate, Fetch data for that ticker and date
            Scanner in = new Scanner(System.in);
            while (true) {
                System.out.print("Enter ticker and date (YYYY.MM.DD): ");
                String[] data = in.nextLine().trim().split("\\s+");
                if (data.length < 2)
                    break;
                showTickerDay(data[0], data[1]);
            }

            conn.close();
        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                                    ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
        }
    }

    static void showCompanies() throws SQLException {
        // Create and execute a query
        Statement stmt = conn.createStatement();
        ResultSet results = stmt.executeQuery("select Ticker, Name from Company");

        // Show results
        while (results.next()) {
            System.out.printf("%5s %s%n", results.getString("Ticker"), results.getString("Name"));
        }
        stmt.close();
    }

    static void showTickerDay(String ticker, String date) throws SQLException {
        // Prepare query
        PreparedStatement pstmt = conn.prepareStatement(
                "select OpenPrice, HighPrice, LowPrice, ClosePrice " +
                "  from PriceVolume " +
                "  where Ticker = ? and TransDate = ?");

        // Fill in the blanks
        pstmt.setString(1, ticker);
        pstmt.setString(2, date);
        ResultSet rs = pstmt.executeQuery();

        // Did we get anything? If so, output data.
        if (rs.next()) {
            System.out.printf("Open: %.2f, High: %.2f, Low: %.2f, Close: %.2f%n",
                    rs.getDouble(1), rs.getDouble(2), rs.getDouble(3), rs.getDouble(4));
        } else {
            System.out.printf("Ticker %s, Date %s not found.%n", ticker, date);
        }
        pstmt.close();
    }
}
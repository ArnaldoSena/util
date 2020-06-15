package sqltocsv;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.opencsv.CSVWriter;

public class DbToCSV {
	public static void main(String args[]) throws SQLException, IOException {
	      //Getting the connection
	      String url = "jdbc:postgresql://localhost/mydb";
	      Connection con = DriverManager.getConnection(url, "postgres", "postgres");
	      System.out.println("Connection established......");
	      //Creating the Statement
	      Statement stmt = con.createStatement();
	      //Query to retrieve records
	      String query = "Select * from empDetails";
	      //Executing the query
	      //stmt.executeQuery("use mydb");
	      ResultSet rs = stmt.executeQuery(query); 
	      //Instantiating the CSVWriter class
	      CSVWriter writer = new CSVWriter(new FileWriter("output1.csv"));
	      ResultSetMetaData Mdata = rs.getMetaData();
	      int totalColunas = Mdata.getColumnCount();
	      String linha1[] = new String[totalColunas];
	      for(int i = 1; i <= totalColunas; i++) {
	    	  	linha1[i-1] = Mdata.getColumnLabel(i);
	      }
	      writer.writeNext(linha1);
	      
	      String data[] = new String[totalColunas];
	      while(rs.next()) {
	    	  	for(int i = 0; i < totalColunas; i++) {
	    	  		int tipo = rs.getMetaData().getColumnType(i+1);
	    	  		switch(tipo){
	    	  			case java.sql.Types.INTEGER	: data[i] = String.valueOf(rs.getInt(i+1)); break;
	    	  			case java.sql.Types.SMALLINT	: data[i] = String.valueOf(rs.getShort(i+1)); break;
	    	  			case java.sql.Types.BIGINT	: data[i] = new Long(rs.getInt(i+1)).toString(); break;
	    	  			case java.sql.Types.REAL		: data[i] = new Float(rs.getFloat(i+1)).toString(); break;
	    	  			case java.sql.Types.DOUBLE	: data[i] = new Double(rs.getDouble(i+1)).toString(); break;
	    	  			case java.sql.Types.BOOLEAN	: data[i] = new Boolean(rs.getBoolean(i+1)).toString(); break;
	    	  			case java.sql.Types.BIT		: data[i] = new Boolean(rs.getBoolean(i+1)).toString(); break;
	    	  			case java.sql.Types.DECIMAL	: data[i] = new BigDecimal(rs.getBigDecimal(i+1).toBigInteger()).toString(); break;
	    	  			case java.sql.Types.NUMERIC	: data[i] = new BigDecimal(rs.getBigDecimal(i+1).toBigInteger()).toString(); break;
	    	  			case java.sql.Types.DATE		: data[i] = rs.getDate(i+1).toString(); break;
	    	  			case java.sql.Types.TIME		: data[i] = rs.getTime(i+1).toString(); break;
	    	  			case java.sql.Types.VARCHAR 	: data[i] = rs.getString(i+1); break;
	    	  			case java.sql.Types.NVARCHAR	: data[i] = rs.getString(i+1); break;
	    	  			case java.sql.Types.CHAR		: data[i] = rs.getString(i+1); break;
	    	  			case java.sql.Types.TIMESTAMP : data[i] = rs.getTimestamp(i+1).toString(); break;
	    	  			case java.sql.Types.LONGNVARCHAR : data[i] = rs.getString(i+1); break;
	    	  			default : data[i] = rs.getString(i+1);
	    	  		}
	    	  	}
    	  		writer.writeNext(data);
	      }
	      writer.flush();
	      System.out.println("Data entered");
	      writer.close();
	}
}

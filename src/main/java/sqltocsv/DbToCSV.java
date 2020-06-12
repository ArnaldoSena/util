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
	      
	      
//	      Mdata.getColumnName(1);
//	      //Writing data to a csv file
//	      String line1[] = {Mdata.getColumnName(1), Mdata.getColumnName(2), Mdata.getColumnName(3), Mdata.getColumnName(4), Mdata.getColumnName(5)};
//	      writer.writeNext(line1);
	      
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
	    	  			case java.sql.Types.DATE		: data[i] = rs.getDate(i+1).toString();
	    	  			case java.sql.Types.TIME		: data[i] = rs.getTime(i+1).toString();
	    	  			case java.sql.Types.TIMESTAMP : data[i] = rs.getTimestamp(i+1).toString();
	    	  			case java.sql.Types.VARCHAR 	: data[i] = rs.getString(i+1); break;
	    	  			case java.sql.Types.NVARCHAR	: data[i] = rs.getString(i+1); break;
	    	  			case java.sql.Types.CHAR		: data[i] = rs.getString(i+1); break;
	    	  			case java.sql.Types.LONGNVARCHAR : data[i] = rs.getString(i+1); break;
	    	  			default : data[i] = rs.getString(i+1);
	    	  		}
	    	  	}
    	  		writer.writeNext(data);

//	    	  	int tipo = java.sql.Types.INTEGER;
//	    	  	System.out.println("Tipo = " + tipo);
//	    	  	System.out.print(rs.getMetaData().getColumnTypeName(1) + "|| ");
//	    	  	System.out.print(rs.getMetaData().getColumnType(1) + ", ");
//	         data[0] = new Integer(rs.getInt("ID")).toString();
//	    	  	System.out.print(rs.getMetaData().getColumnTypeName(2) + "|| ");
//	    	  	System.out.print(rs.getMetaData().getColumnType(2) + ", ");
//	         data[1] = rs.getString("Name");
//	    	  	System.out.print(rs.getMetaData().getColumnTypeName(3) + ", ");
//	         data[2] = new Integer(rs.getInt("Salary")).toString();
//	    	  	System.out.print(rs.getMetaData().getColumnTypeName(4) + ", ");
//	         data[3] = rs.getString("start_date");
//	    	  	System.out.print(rs.getMetaData().getColumnTypeName(5) + ", ");
//	         data[4] = rs.getString("Dept");
	         

	      }
	      //Flushing data from writer to file
	      writer.flush();
	      System.out.println("Data entered");
	}
}

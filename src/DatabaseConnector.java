/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author 
 */
public class DatabaseConnector {
    public Connection conn = null;
    private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(DatabaseConnector.class);
	
    public DatabaseConnector(){
        try {
            Class.forName("com.mysql.jdbc.Driver");
            
            String sysURL = "jdbc:mysql://127.0.0.1:3306/sawapay_db";
            conn = DriverManager.getConnection(sysURL, "root", "");
            logger.info("Connection"+conn);
            
            //String sysURL = "jdbc:mysql://localhost/sawapay_db";
            //conn = DriverManager.getConnection(sysURL,"root","");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DatabaseConnector.class.getName()).log(Level.SEVERE, null, ex);
            logger.error("Error2"+ex.toString());
            conn = null;
        }catch(SQLException ex){
        	Logger.getLogger(DatabaseConnector.class.getName()).log(Level.SEVERE, null, ex);
            logger.error("Error1"+ex.toString());
            conn = null;
        } 
   }
    
    /***
    * 
    * @return Connection conn
    */
   public Connection getConn(){
        return conn;
   }
   
   public void closeConnect() {
        try {
            System.out.println("Closing database connections");
            conn.close();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseConnector.class.getName()).log(Level.WARNING, null, ex);
            logger.info("Error"+ex.toString());
        }
    }
}

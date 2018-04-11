import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;









import com.mysql.jdbc.Connection;


public class EmailAlert {
	private static Logger logger=Logger.getLogger(EmailAlert.class);
	    
	    static final String JDBC_DRIVER="com.mysql.jdbc.Driver";
	    
	    //static final String DB_URL="jdbc:mysql://10.229.176.12/sawapay_db";
	    static final String DB_URL="jdbc:mysql://127.0.0.1/sawapay_db";
	    
	    //static final String USER="sly";
	    static final String USER="root";
	    
	    static final String PASS="";
	    //static final String PASS="sly123@";
	    
	    private static DatabaseConnector dbConnector;
	    private static java.sql.Connection conn;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("/Users/mac/Documents/workspace/EmailAlertSystem/log4j.xml");
		BasicConfigurator.configure();
		
		//create Connection
		logger.info("Here");
		 conn = getDBConnection();
	        if(null == conn){
	        	logger.info("Here one");
	        	
	        	logger.info("Main Thread: SQL Exception occurred! Newly started Instance exiting.!!");
				System.exit(1);
	  	      }
	       
	        logger.info("Here Again");
		
	
		
		int MINUTES = 1; // The delay in minutes
		Timer timer = new Timer();
		 timer.schedule(new TimerTask() {
		    @Override
		    public void run() { // Function runs every MINUTES minutes.
		        // Run the code you want here
		    	try {
		    		new EmailAlert().getSystemStatus(conn);
		    		conn.close();
					
					
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					logger.info("Exception"+e1);
					//e1.printStackTrace();
				}	
		    }

			

			
		 }, 0, 1000 * 60 * MINUTES);

	}
	private void getSystemStatus(java.sql.Connection conn1) throws InterruptedException {
		// TODO Auto-generated method stub
		try{
			while(true){
				
				Statement smt1;
			   int alert_code=1;
				logger.info("Response Thread: DB connection established"); 
				
				String checkSystemStatusQuery="SELECT id,active,app_name FROM conf_system_status where app_name = 'EmailAlertSys'";
				smt1=conn1.createStatement();
				ResultSet rc=smt1.executeQuery(checkSystemStatusQuery);
				if (!rc.isBeforeFirst() ) {    
				    logger.info("NO Application with the given name Found!");    
				} 
				  int system_id;
				  int h =0;
				  String appname = null;
				while(rc.next()){
				  int status=rc.getInt("active");
				  system_id=rc.getInt("id");
				  appname=rc.getString("app_name");
				  if(status ==0){
					  logger.info("System Status: System deactivated...");
		                System.exit(1);
				  }
				  String updateQueryApp = "UPDATE conf_system_status set last_time_started = current_timestamp, start_count = start_count + ?, active = ? WHERE id = ?";
	                PreparedStatement stmtupdateapp = conn.prepareStatement(updateQueryApp);
	                stmtupdateapp.setInt(1,1);
	                stmtupdateapp.setInt(2,1);
	                stmtupdateapp.setInt(3, system_id);
	                logger.info("Main Thread: Update Query: "+stmtupdateapp.toString());
	                stmtupdateapp.executeUpdate();
	                logger.info("Main Thread: Update Query Executed!");
	                h++;   	
				}
				rc.close();
				smt1.close();
				if(0 == h){
	                try {
	                    conn.close();
	                    logger.info("Closing DB connection!");
	                } catch (SQLException e) {
	                    // TODO Auto-generated catch block
	                    e.printStackTrace();
	                } 
	                logger.info("Main Thread: Newly started Instance exiting...");
	                System.exit(1);
	             }
				
				updateSystemStatus(conn1,appname); 
				Statement smt2;
				
				String  selectQuery="SELECT id,recipient_msisdn, receive_amount, b2m_result_code, disbursement_method, payment_method  FROM core_money_transfer WHERE (status = 3 AND (b2m_result_code != 0 OR b2m_result_code !=NULL ) ) AND email_fail_alert_flag =0 LIMIT 10";
				smt2=conn1.createStatement();
				// fetching records
				ResultSet rs=smt2.executeQuery(selectQuery);
				if (!rs.isBeforeFirst() ) {     
				    logger.info("NO Transaction ID Found");    
				} 
				String recipient_msisdn;
				String disbursement_method;
				String amount;
				String result_code;
				String payment_method;
				
				String result_response;
				  
				while(rs.next()){
					
					
				  int  id=rs.getInt("id");
				    recipient_msisdn=rs.getString("recipient_msisdn");
				    amount=rs.getString("receive_amount");
				    disbursement_method=rs.getString("disbursement_method");
				    result_code=rs.getString("b2m_result_code");
				    payment_method=rs.getString("payment_method");
				   logger.info(" Transaction with ID "+id+ " Found");
				  
				   //logger.info(recipient_msisdn);
				   
				   if(payment_method.equalsIgnoreCase("1")){
					   payment_method = "E-Wallet";
				   }else if(payment_method.equalsIgnoreCase("2")){
					   payment_method = "Bank";
				   }
				   else if(payment_method.equalsIgnoreCase("3")){
					   payment_method = "Card";
				   }else {
					   payment_method = "Unrecognised Payment Method";
				   }
				   
				   if(disbursement_method.equalsIgnoreCase("1")){
					   disbursement_method = "Mpesa";
				   }else if(disbursement_method.equalsIgnoreCase("2")){
					   disbursement_method = "PayBill";
				   }
				   else if(disbursement_method.equalsIgnoreCase("3")){
					   disbursement_method = "Kenyan Bank";
				   }else if(disbursement_method.equalsIgnoreCase("4")){
					   disbursement_method = "Uganda Airtel Money";
				   }else if(disbursement_method.equalsIgnoreCase("5")){
					   disbursement_method = "Uganda MTN Money";
				   }else if(disbursement_method.equalsIgnoreCase("6")){
					   disbursement_method = "Zimbabwe Telecash";
				   }else if(disbursement_method.equalsIgnoreCase("7")){
					   disbursement_method = "Ethiopian CBE Mobile Wallet";
				   }else {
					   disbursement_method = "Unrecognised Disbursement Method";
				   }
				   
				   if(result_code.equalsIgnoreCase("1")){
					   result_response = "Pending";
				   }else if(result_code.equalsIgnoreCase("2")){
					   result_response = "Queued";
				   }
				   else if(result_code.equalsIgnoreCase("3")){
					   result_response = "Processed";
				   }else if(result_code.equalsIgnoreCase("34")){
					   result_response = "Rejected Transaction";
				   }else if(result_code.equalsIgnoreCase("-8")){
					   result_response = "Card Error";
				   }else if(result_code.equalsIgnoreCase("-11")){
					   result_response = "Failed";
				   }else if(result_code.equalsIgnoreCase("11")){
					   result_response = "Credit in Progress";
				   }else if(result_code.equalsIgnoreCase("29")){
					   result_response = "Incomplete Processing";
				   }else if(result_code.equalsIgnoreCase("-15")){
					   result_response = "Insufficient Funds";
				   }else {
					   result_response = "Unknown Error";
				   }
				   
				   
				   
				   
				   
				   
				   updateEmailAlert(conn1,id,alert_code);
				   
					try {
						
						 logger.info("... Initiating Send Email Request....\n");
						new EmailAlert().SendEmail(conn1,id,amount,payment_method,disbursement_method,result_response);
						
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
	                 	
				}
				
				 rs.close();
				smt2.close();
			    Thread.sleep(10000); 	
			}
		}catch(SQLException e){
			e.printStackTrace();
		}
		
	}
	private void updateSystemStatus(java.sql.Connection conn1, String appname) {
		// TODO Auto-generated method stub
		
		try{
			Statement smt1;
			  
			 int    system_status =0;
			
			String  updateQuery="UPDATE conf_system_status set date_modified = current_timestamp WHERE app_name='"+appname+"'";
			smt1=conn1.createStatement();
			// updating system status
			smt1.execute(updateQuery);
			system_status++;
			
			System.out.println("System updated ...");
			logger.info("System updated ...");
		
		
			smt1.close();
			
			 if(0 == system_status){
				 try {
					
	              conn1.close();
	                    logger.info("Closing DB connection!");
	                } catch (SQLException e) {
	                    // TODO Auto-generated catch block
	                    e.printStackTrace();
	                } 
	                logger.info("Main Thread: Newly started Instance exiting...");
	                System.exit(1);
				   }
			
		
	}catch(SQLException e){
		logger.info(e.toString());
		
		}

		
	}
	private static void updateEmailAlert(java.sql.Connection conn1, int id,
			int alert_code) {
		// TODO Auto-generated method stub
		try{
			
			
			String update ="UPDATE core_money_transfer SET email_fail_alert_flag = ?  "
				 	+ "WHERE id = ? ";
			 java.sql.PreparedStatement smt1 = conn1.prepareStatement(update);
			 smt1.setInt(1, alert_code);
			 smt1.setInt(2, id);
			 smt1.executeUpdate();
			 logger.info(update);
		
		logger.info("Email Alert Flaf of ID "+id +" updated ... with Alert Code "+alert_code);
	
	
		smt1.close();
	
}catch(SQLException e){
	logger.info(e.toString());
	
	}
		
	}
	private void SendEmail(java.sql.Connection conn1, int id,
			 String amount,String payment_method,  String disbursement_method, String result_response) {
		// TODO Auto-generated method stub
		
		String message="A "+payment_method+ " to "+disbursement_method+" Transanction with id " +id+" and Amount "+amount+ " FAILED. with message "+result_response+"  \n\n";
		   String subject=payment_method+" Transaction Failed";
		   String sender_name="SawaPay Transaction Fail Alert";
		   String sender_email="customercare@sawa-pay.com";
		    String recipient_email="customercare@sawa-pay.com";
		   String bcc="smwambeke@firstchoicegl.com";
		   String cc="ian@bongatech.co.ke,morris.murega@firstchoicegl.com,alan@firstchoicegl.com";
		   
		   logger.info(subject+ "/n"+message);
		   
		   
		   try{
				
				
			   String query = "INSERT INTO email_outbox_queue(message,subject,sender_name,sender_email,recipient_email, bcc,cc)  VALUES(?, ?, ?, ?, ?, ?,?)";

				      // create the mysql insert preparedstatement
				      PreparedStatement preparedStmt = conn1.prepareStatement(query);
				      preparedStmt.setString (1, message);
				      preparedStmt.setString (2, subject);
				      preparedStmt.setString   (3, sender_name);
				      preparedStmt.setString(4, sender_email);
				      preparedStmt.setString    (5, recipient_email);
				      preparedStmt.setString    (6, bcc);
				      preparedStmt.setString    (7, cc);
				    

				      // execute the preparedstatement
				      preparedStmt.execute();
				      logger.info("Email Alert  of Transaction ID "+id +" Successfully  Submited... with Message "+message);
				      
		
	}catch(SQLException e){
		logger.info(e.toString());
		
		}
		
	}
	private static java.sql.Connection getDBConnection() {
		// TODO Auto-generated method stub
		java.sql.Connection dbConnection = null;

		try {

			Class.forName(JDBC_DRIVER);

		} catch (ClassNotFoundException e) {

			logger.info(e.getMessage());

		}
		logger.info("USer"+USER);

		try {

			dbConnection = DriverManager.getConnection(
					DB_URL,USER,PASS);
			//return dbConnection;
			logger.info("USer"+USER);

		} catch (SQLException e) {

			logger.info("Exception1 "+e.toString());

		}

		return dbConnection;
	}

}

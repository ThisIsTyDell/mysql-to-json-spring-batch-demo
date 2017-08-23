package com.capgemini.dbtojson;

import java.util.List;

//import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
//import org.springframework.batch.item.ExecutionContext;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.jdbc.core.BeanPropertyRowMapper;
//import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener implements JobExecutionListener {
	
	//@Autowired
	//public DataSource dataSource;
	
	private static Logger log = LogManager.getLogger();
	
    @Override
    public void beforeJob(JobExecution jobExecution) {
    	//JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    	//List<Customer> customerList = jdbcTemplate.query("SELECT * FROM customers",
    	//		new BeanPropertyRowMapper<Customer>(Customer.class));
   
    	//ExecutionContext jobContext = jobExecution.getExecutionContext();
    	//jobContext.put("customerList", customerList);
    	
    	log.info("!!! JOB STARTED!");
    }
	
	@Override
    public void afterJob(JobExecution jobExecution) {
        if(jobExecution.getStatus() == BatchStatus.COMPLETED){
        	log.info("!!! JOB FINISHED! Time to verify the results");
        }else if(jobExecution.getStatus() == BatchStatus.FAILED){
        	log.info("!!! DB to CSV job failed with following exceptions");
        	
            List<Throwable> exceptionList = jobExecution.getAllFailureExceptions();
            for(Throwable th : exceptionList){
                log.error("exception :" +th.getLocalizedMessage());
            }
        }
    }

}

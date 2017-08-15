package com.example.demo;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener implements JobExecutionListener {
	
	private static Logger log = LogManager.getLogger();
	
    @Override
    public void beforeJob(JobExecution jobExecution) {
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
                System.err.println("exception :" +th.getLocalizedMessage());
            }
        }
    }

}

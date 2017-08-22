package com.capgemini.dbtojson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.file.transform.LineAggregator;

import com.google.gson.Gson;

public class JsonLineAggreator<Customer> implements LineAggregator<Customer>, StepExecutionListener {

	private Gson gson = new Gson();
	private boolean isFirstObject = true;
	private static Logger log = LogManager.getLogger();

	
	@Override
	public String aggregate(final Customer customer) {
		if(isFirstObject) {
			isFirstObject = false;
			return gson.toJson(customer);
		}
		log.info("Converting " + customer.toString() + " to JSON Object");
		return "," + gson.toJson(customer);
	}
	
	@Override
	public void beforeStep(final StepExecution stepExecution) {
		if(stepExecution.getExecutionContext().containsKey("isFirstObject")) {
			isFirstObject = Boolean.parseBoolean(stepExecution.getExecutionContext().getString("isFirstObject"));
		}
	}

	@Override
	public ExitStatus afterStep(final StepExecution stepExecution) {
		stepExecution.getExecutionContext().putString("isFirstObject", Boolean.toString(isFirstObject));
		return null;
	}

}

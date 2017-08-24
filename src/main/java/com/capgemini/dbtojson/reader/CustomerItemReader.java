package com.capgemini.dbtojson.reader;

import java.util.Comparator;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.capgemini.dbtojson.models.Customer;

public class CustomerItemReader implements ItemReader<Customer> {
	
	@Autowired
	public DataSource dataSource;
	
	private int nextCustomerIndex;
	private List<Customer> customerList;	

	@Override
	public Customer read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		Customer nextCustomer = null;
		
		if (nextCustomerIndex < customerList.size()) {
			nextCustomer = customerList.get(nextCustomerIndex);
			nextCustomerIndex++;
		}
		
		return nextCustomer;
	}
	
	/**
	 * Queries the database to get Customer data, store into a list, and sort before beginning step.
	 * 
	 */
	@BeforeStep
	public void getCustomerList() {
		JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    	this.customerList = jdbcTemplate.query("SELECT * FROM customers",
    			new BeanPropertyRowMapper<Customer>(Customer.class));	
    	
    	customerList.sort(Comparator.comparing(Customer::getFirstName));
	}

}

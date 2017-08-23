package com.capgemini.dbtojson.configuration;

// import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
// import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
// import org.springframework.jdbc.core.BeanPropertyRowMapper;

import com.capgemini.dbtojson.jsonwriter.JsonFlatFileFooterCallback;
import com.capgemini.dbtojson.jsonwriter.JsonFlatFileHeaderCallback;
import com.capgemini.dbtojson.jsonwriter.JsonLineAggreator;
import com.capgemini.dbtojson.models.Customer;
import com.capgemini.dbtojson.reader.CustomerItemReader;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	@Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    // @Autowired
    // public DataSource dataSource;
	
    // private static final String QUERY_FIND_CUSTOMERS = "SELECT * FROM customers";
    
    // @Bean
    // ItemReader<Customer> databaseItemReader(DataSource dataSource) {
        // JdbcCursorItemReader<Customer> databaseReader = new JdbcCursorItemReader<>();
 
        // databaseReader.setDataSource(dataSource);
        // databaseReader.setSql(QUERY_FIND_CUSTOMERS);
        // databaseReader.setRowMapper(new BeanPropertyRowMapper<>(Customer.class));
 
        // return databaseReader;
    // }
    
    @Bean
    ItemReader<Customer> customerItemReader() {
		return new CustomerItemReader();
    	
    }
    
    @Bean
    ItemWriter<Customer> jsonWriter() {
    	FlatFileItemWriter<Customer> jsonFileWriter = new FlatFileItemWriter<>();
    
    	String exportFilePath = "customers.json";
    	jsonFileWriter.setResource(new FileSystemResource(exportFilePath));
    
    	JsonLineAggreator<Customer> jsonAggreator = new JsonLineAggreator<>();
    	jsonFileWriter.setLineAggregator(jsonAggreator);
    	
    	JsonFlatFileHeaderCallback headerWriter = new JsonFlatFileHeaderCallback();
    	jsonFileWriter.setHeaderCallback(headerWriter);
    	
    	JsonFlatFileFooterCallback footerWriter = new JsonFlatFileFooterCallback();
    	jsonFileWriter.setFooterCallback(footerWriter);
    
    	return jsonFileWriter;
    }
	
    /**
     * 
     * @param listener
     * @return
     */
	@Bean
    public Job importUserJob(JobCompletionNotificationListener listener) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(exportToJSON())
                .end()
                .build();
    }
    
	/**
	 * 
	 * @return
	 */
    @Bean
    public Step exportToJSON() {
    	return stepBuilderFactory.get("exportToJSON")
    			.<Customer,Customer> chunk(10)
    			// .reader(databaseItemReader(dataSource))
    			.reader(customerItemReader())
    			.writer(jsonWriter())
    			.build();
    }
}
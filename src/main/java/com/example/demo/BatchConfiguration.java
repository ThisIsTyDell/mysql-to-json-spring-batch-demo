package com.example.demo;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	@Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

	
    private static final String QUERY_FIND_CUSTOMERS = "SELECT * FROM customers";
    
    @Bean
    ItemReader<Customer> databaseItemReader(DataSource dataSource) {
        JdbcCursorItemReader<Customer> databaseReader = new JdbcCursorItemReader<>();
 
        databaseReader.setDataSource(dataSource);
        databaseReader.setSql(QUERY_FIND_CUSTOMERS);
        databaseReader.setRowMapper(new BeanPropertyRowMapper<>(Customer.class));
 
        return databaseReader;
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

	@Bean
	ItemWriter<Customer> databaseCSVItemWriter() {
		FlatFileItemWriter<Customer> csvFileWriter = new FlatFileItemWriter<>();
		
		String exportFileHeader = "FIRST NAME,LAST NAME,PHONE NUMBER,CITY,ZIP CODE";
		StringHeaderWriter headerWriter = new StringHeaderWriter(exportFileHeader);
		csvFileWriter.setHeaderCallback(headerWriter);
		
		String exportFilePath = "customers.csv";
		csvFileWriter.setResource(new FileSystemResource(exportFilePath));
		
		LineAggregator<Customer> lineAggregator = createCustomerLineAggregator();
		csvFileWriter.setLineAggregator(lineAggregator);
		
		return csvFileWriter;
	}
	
	private LineAggregator<Customer> createCustomerLineAggregator() {
		DelimitedLineAggregator<Customer> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");
		
		FieldExtractor<Customer> fieldExtractor = createCustomerFieldExtractor();
		lineAggregator.setFieldExtractor(fieldExtractor);
		
		return lineAggregator;
	}

	private FieldExtractor<Customer> createCustomerFieldExtractor() {
		BeanWrapperFieldExtractor<Customer> extractor = new BeanWrapperFieldExtractor<>();
		extractor.setNames(new String[] {"firstName", "lastName", "phoneNumber", "city", "zipCode"});
		return extractor;
	}
	
	@Bean
    public Job importUserJob(JobCompletionNotificationListener listener) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(exportToCSV())
                .next(exportToJSON())
                .end()
                .build();
    }

    @Bean
    public Step exportToCSV() {
        return stepBuilderFactory.get("exportToCSV")
                .<Customer, Customer> chunk(10)
                .reader(databaseItemReader(dataSource))
                .writer(databaseCSVItemWriter())
                .build();
    }
    
    @Bean
    public Step exportToJSON() {
    	return stepBuilderFactory.get("exportToJSON")
    			.<Customer,Customer> chunk(10)
    			.reader(databaseItemReader(dataSource))
    			.writer(jsonWriter())
    			.build();
    }
}
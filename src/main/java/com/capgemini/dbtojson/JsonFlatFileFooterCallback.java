package com.capgemini.dbtojson;

import java.io.IOException;
import java.io.Writer;

import org.springframework.batch.item.file.FlatFileFooterCallback;

public class JsonFlatFileFooterCallback implements FlatFileFooterCallback {

	@Override
	public void writeFooter(final Writer writer) throws IOException {
		writer.write("]");
	}

}

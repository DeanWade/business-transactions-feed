package com.dynatrace.diagnostics.core.realtime.flume.elasticsearch;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.elasticsearch2.*;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.dynatrace.diagnostics.core.realtime.export.BtExport.BtOccurrence;
import com.dynatrace.diagnostics.core.realtime.export.BtExport.BusinessTransaction;

public class BtExportElasticSearchEventSerializer implements ElasticSearchEventSerializer {


//	private final static Logger log = LoggerFactory.getLogger(BtExportElasticSearchEventSerializer.class);
	
//	private final static Charset charset = Charset.forName("UTF-8");
	
//	private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SZ");


	@Override
	public BytesReference getContentBuilder(Event event) throws IOException {
		BusinessTransaction bt = BusinessTransaction.parseFrom(event.getBody());
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
		appendHeaders(builder, bt);
	    appendBody(builder, bt);
		return builder.bytes();
	} 

	private void appendHeaders(XContentBuilder builder, BusinessTransaction bt) throws IOException {
		
		if(bt.getOccurrencesCount() == 0){
			throw new IOException("data is missing: no occurrence");
		}
		
		BtOccurrence occurrence= bt.getOccurrences(0);
		
		if (bt.hasName()) {
			builder.field("name",bt.getName().toLowerCase());
		}
		if (bt.hasType()) {
			builder.field("type",bt.getType().name().toLowerCase());
		}

		if (occurrence.hasPurePathId()) {
			builder.field("id",occurrence.getPurePathId());
		}
		if (bt.hasApplication()) {
			builder.field("application",bt.getApplication());
		}
		if (bt.hasSystemProfile()) {
			builder.field("systemProfile",bt.getSystemProfile());
		}
		
		if (occurrence.hasClientIP()) {
			builder.field("clientIP", occurrence.getClientIP());
		}
		
		if (occurrence.hasStartTime()) {
			builder.field("startTime", new Date(occurrence.getStartTime()));
		}

		if (occurrence.hasEndTime()) {
			builder.field("endTime", new Date(occurrence.getEndTime()));
		}
		
		if (occurrence.hasResponseTime()) {
			builder.field("responseTime",occurrence.getResponseTime());
		}
		if (occurrence.hasDuration()) {
			builder.field("duration",occurrence.getDuration());
		}

		if (occurrence.hasCpuTime()) {
			builder.field("cpuTime",occurrence.getCpuTime());
		}

		if (occurrence.hasExecTime()) {
			builder.field("execTime",occurrence.getExecTime());
		}

		if (occurrence.hasSuspensionTime()) {
			builder.field("suspensionTime",occurrence.getSuspensionTime());
		}

		if (occurrence.hasSyncTime()) {
			builder.field("syncTime",occurrence.getSyncTime());
		}

		if (occurrence.hasWaitTime()) {
			builder.field("waitTime",occurrence.getWaitTime());
		}
		
		if (occurrence.hasFailed()) {
			builder.field("failed", occurrence.getFailed());
		}
		
		int dimensionNamesCount = bt.getDimensionNamesCount();
		int dimensionsCount = occurrence.getDimensionsCount();
		for (int i = 0; i < dimensionNamesCount && i < dimensionsCount ; i++) {
			builder.field(bt.getDimensionNames(i),	occurrence.getDimensions(i));
		}
		
		int measureNamesCount = bt.getMeasureNamesCount();
		int valuesCount = occurrence.getValuesCount();
		for (int i = 0; i < measureNamesCount && i < valuesCount; i++) {
			builder.field(bt.getMeasureNames(i),	occurrence.getValues(i));
		}
	}

	private void appendBody(XContentBuilder builder, BusinessTransaction bt) {
	}

	@Override
	public void configure(Context context) {
		
	}

	@Override
	public void configure(ComponentConfiguration componentConfiguration) {
		
	}

}

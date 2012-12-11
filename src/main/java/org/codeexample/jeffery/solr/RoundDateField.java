package org.codeexample.jeffery.solr;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.lucene.index.IndexableField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;

public class RoundDateField extends TrieDateField {

	/**
	 * roundTo can be predefined format, such as day, second, or it can be a
	 * format string, such as yyyy-MM-dd'T'hh:00:00.0'Z'
	 */
	private String roundTo = "roundTo";
	private static String ROUND_TO_DAY = "DAY";
	private static String FORMAT_DAY = "yyyy-MM-dd'T'00:00:00.0'Z'";

	// yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
	private static String ROUND_SECOND = "SECOND";
	private static String FORMAT_SECOND = "yyyy-MM-dd'T'HH:mm:ss'Z'";

	@Override
	protected void init(IndexSchema schema, Map<String, String> args) {
		if (args != null) {
			roundTo = args.remove(roundTo);
		}
		super.init(schema, args);
	}

	@Override
	public IndexableField createField(SchemaField field, Object value,
			float boost) {

		try {
			// Value is a string: yyyy-MM-dd'T'HH:mm:ss'Z'
			Date date = parseSolrDate(value.toString());
			DateFormat df = null;

			if (ROUND_TO_DAY.equalsIgnoreCase(roundTo)) {
				df = new SimpleDateFormat(FORMAT_DAY);
			} else if (ROUND_SECOND.equalsIgnoreCase(roundTo)) {
				df = new SimpleDateFormat(FORMAT_SECOND);
			} else {
				// roundTo can also be format such as yyyy-MM-dd'T'hh:00:00.0'Z'
				df = new SimpleDateFormat(roundTo);
			}
			if (df != null) {
				value = df.format(date);
			}

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}

		return super.createField(field, value, boost);
	}

	public Date parseSolrDate(String dateString) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

		return sdf.parse(dateString);
	}
}

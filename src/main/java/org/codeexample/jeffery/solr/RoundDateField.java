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
	  
	  private SimpleDateFormat sdf;
	  
	  private String fromFormat = null;
	  private static String PARAM_FROM_FORMAT = "fromFormat";
	  private static String DATE_FORMAT_UNIX_SECOND = "UNIX_SECOND";
	  private static String DATE_FORMAT_UNIX_MILLSECOND = "UNIX_MILLSECOND";
	  
	  private static long MS_IN_DAY = 3600 * 24 * 1000;
	  private static final long SECONDS_FROM_EPCO = new Date().getTime() / 1000;
	  
	  @Override
	  protected void init(IndexSchema schema, Map<String,String> args) {
	    if (args != null) {
	      fromFormat = args.remove(PARAM_FROM_FORMAT);
	    }
	    sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
	    sdf.setTimeZone(UTC);
	    super.init(schema, args);
	  }
	  
	  /**
	   * if value > SECONDS_FROM_EPCO, then treat value as milliseconds, otherwise
	   * treat value as seconds
	   * 
	   * @param value
	   * @return
	   */
	  private long convertToMillseconds(long value) {
	    long result = value;
	    if (value < SECONDS_FROM_EPCO) {
	      result = result * 1000L;
	    }
	    return result;
	  }
	  
	  @Override
	  public IndexableField createField(SchemaField field, Object value, float boost) {
	    
	    try {
	      long millseconds = -1;
	      
	      try {
	        millseconds = Long.parseLong(value.toString());
	        
	        if (fromFormat != null) {
	          if (DATE_FORMAT_UNIX_MILLSECOND.equalsIgnoreCase(fromFormat)) {
	            // do nothing
	          } else if (DATE_FORMAT_UNIX_SECOND.equalsIgnoreCase(fromFormat)) {
	            millseconds = millseconds * 1000L;
	          } else {
	            throw new RuntimeException("Invalid fromFormat: " + fromFormat);
	          }
	        } else {
	          millseconds = convertToMillseconds(millseconds);
	        }
	        
	      } catch (Exception ex) {
	        // so it should be a date string
	        millseconds = sdf.parse(value.toString()).getTime();
	      }
	      
	      millseconds = (millseconds / MS_IN_DAY) * MS_IN_DAY + (MS_IN_DAY / 2);
	      // returned value must be a date time string
	      value = new Date(millseconds);
	    } catch (Exception ex) {
	      throw new RuntimeException(ex);
	    }
	    
	    return super.createField(field, value, boost);
	  }
}

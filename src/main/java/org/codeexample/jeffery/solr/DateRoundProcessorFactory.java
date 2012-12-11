package org.codeexample.jeffery.solr;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

public class DateRoundProcessorFactory extends UpdateRequestProcessorFactory {

	private List<String> dateFields;
	/**
	 * Each value can be predefined format, such as day, second, or it can be a
	 * format string, such as yyyy-MM-dd'T'hh:00:00.0'Z'
	 */
	private List<String> dateRoundFields;
	// ignoreError
	private boolean ignoreError;

	private static String ROUND_DAY = "DAY";
	private static String FORMAT_DAY = "yyyy-MM-dd'T'00:00:00.0'Z'";

	// yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
	private static String ROUND_SECOND = "SECOND";
	private static String FORMAT_SECOND = "yyyy-MM-dd'T'HH:mm:ss'Z'";

	@SuppressWarnings("rawtypes")
	@Override
	public void init(final NamedList args) {
		if (args != null) {
			SolrParams params = SolrParams.toSolrParams(args);

			Object fields = args.get("date.fields");
			dateFields = fields == null ? null : StrUtils.splitSmart(
					(String) fields, ",", true);

			fields = args.get("date.round.fields");
			dateRoundFields = fields == null ? null : StrUtils.splitSmart(
					(String) fields, ",", true);

			if ((dateFields == null && dateRoundFields != null)
					|| (dateFields != null && dateRoundFields == null)
					|| (dateFields != null && dateRoundFields != null
							& dateFields.size() != dateRoundFields.size()))
				throw new IllegalArgumentException(
						"Size of date.fields and date.round.fields must be same.");
			ignoreError = params.getBool("ignoreError", false);
		}
	}

	@Override
	public UpdateRequestProcessor getInstance(SolrQueryRequest req,
			SolrQueryResponse rsp, UpdateRequestProcessor next) {
		return new DateRoundProcessor(req, next);
	}

	class DateRoundProcessor extends UpdateRequestProcessor {

		public DateRoundProcessor(SolrQueryRequest req,
				UpdateRequestProcessor next) {
			super(next);
		}

		@Override
		public void processAdd(AddUpdateCommand cmd) throws IOException {

			SolrInputDocument solrInputDocument = cmd.getSolrInputDocument();

			for (int i = 0; i < dateFields.size(); i++) {
				try {
					String dateField = dateFields.get(i);
					SolrInputField inputField = solrInputDocument
							.getField(dateField);

					if (inputField != null) {
						Object obj = inputField.getValue();
						Object result = null;

						if (obj instanceof String) {
							String value = (String) obj;
							Date solrDate = parseSolrDate(value);
							String roundTo = dateRoundFields.get(i);
							DateFormat df = null;
							if (ROUND_DAY.equalsIgnoreCase(roundTo)) {
								df = new SimpleDateFormat(FORMAT_DAY);
							} else if (ROUND_SECOND.equalsIgnoreCase(roundTo)) {
								df = new SimpleDateFormat(FORMAT_SECOND);
							} else {
								// roundTo can also be format such as
								// yyyy-MM-dd'T'hh:00:00.0'Z'
								df = new SimpleDateFormat(roundTo);
							}
							if (df != null) {
								result = df.format(solrDate);
								// only remove it, if there is no error
								solrInputDocument.removeField(dateField);
								solrInputDocument.addField(dateField, result);
							}
						}
					}
				} catch (Exception ex) {
					if (!ignoreError) {
						throw new IOException(ex);
					}
				}
			}

			super.processAdd(cmd);
		}
	}

	public Date parseSolrDate(String dateString) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

		return sdf.parse(dateString);
	}
}

package org.codeexample.jeffery.solr;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QueryResultLRUCache;

public class QueryResultCacheHandler extends RequestHandlerBase {
	protected static final Logger logger = LoggerFactory
			.getLogger(QueryResultCacheHandler.class);

	private static final String PARAM_QUERY = "cachequery";
	private static final String PARAM_ACTION = "action";

	@Override
	public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
			throws Exception {
		QueryResultLRUCache<NamedList<Object>, NamedList<Object>> cvQuerycaCache = req
				.getCore().getQuerycaCache();

		SolrParams params = req.getParams();

		String action = params.get(PARAM_ACTION);
		if (action == null || action.isEmpty()) {
			throw new SolrException(ErrorCode.BAD_REQUEST,
					"action must be set.");
		}

		String cacheQuery = params.get(PARAM_QUERY);
		List<String> queriesLst = new ArrayList<String>();
		if (cacheQuery != null) {
			queriesLst = StrUtils.splitSmart(cacheQuery, ',');
		}

		if ("add".equalsIgnoreCase(action)) {
			boolean persist = params.getBool("cvpersist", false);
			for (String query : queriesLst) {
				cvQuerycaCache.put(query, null, persist);
			}
		} else if ("remove".equalsIgnoreCase(action)) {
			for (String query : queriesLst) {
				cvQuerycaCache.remove(query);
			}
		} else if ("refill".equalsIgnoreCase(action)) {
			boolean sync = params.getBool("sync", false);
			cvQuerycaCache.refill(sync);
		} else {
			throw new SolrException(ErrorCode.BAD_REQUEST, "Unkown action: "
					+ action);
		}
	}

	@Override
	public String getDescription() {
		return null;
	}

	@Override
	public String getSource() {
		return null;
	}

}

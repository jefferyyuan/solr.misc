package org.apache.solr.search;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.SolrRequestParsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryResultLRUCache<K, V> extends
		LRUCache<CacheKeyHashMap, NamedList<Object>> {
	private String description = "My LRU Cache";

	private static final String PROPERTY_FILE = "querycache.properties";
	private static final String PRO_QUERIES = "queries";

	private final Set<String> cachedQueries = new HashSet<String>();
	private SolrCore core;

	protected static Logger logger = LoggerFactory
			.getLogger(QueryResultLRUCache.class);

	public void init(int size, int initialSize, SolrCore core) {
		Map<String, String> args = new HashMap<String, String>();
		args.put("size", String.valueOf(size));
		args.put("initialSize", String.valueOf(initialSize));
		super.init(args, null, regenerator);
		this.core = core;
		cachedQueries.addAll(readCachedQueriesProperty(core));
		for (String query : cachedQueries) {
			CacheKeyHashMap key = convertQueryStringToParams(query, null);
			put(key, null);
			asyncRefill();
		}
	}

	private Set<String> readCachedQueriesProperty(SolrCore core) {
		Set<String> queries = new LinkedHashSet<String>();
		File propertyFile = new File(getPropertyFilePath(core));
		if (propertyFile.exists()) {
			InputStream is = null;
			try {
				is = new FileInputStream(propertyFile);
				Properties properties = new Properties();
				properties.load(is);
				String queriesStr = properties.getProperty(PRO_QUERIES);
				if (queriesStr != null) {
					String[] queriesArray = queriesStr.split(",");
					for (String query : queriesArray) {
						queries.add(query);
					}
				}

			} catch (Exception e) {
				logger.error("Exception happened when read " + propertyFile, e);
			} finally {
				if (is != null) {
					try {
						is.close();
					} catch (IOException e) {
						logger.error("Exception happened when close "
								+ propertyFile, e);
					}
				}
			}
		}

		return queries;
	}

	private void saveCachedQueries() {
		if (!cachedQueries.isEmpty()) {
			File propertyFile = new File(getPropertyFilePath(core));
			OutputStream out = null;
			try {
				out = new FileOutputStream(propertyFile);
				Properties properties = new Properties();
				StringBuilder queries = new StringBuilder(
						16 * cachedQueries.size());
				Iterator<String> it = cachedQueries.iterator();
				while (it.hasNext()) {
					queries.append(it.next());
					if (it.hasNext()) {
						queries.append(",");
					}
				}
				properties.setProperty(PRO_QUERIES, queries.toString());
				properties.store(out, null);
			} catch (Exception e) {
				logger.error("Exception happened when save " + propertyFile, e);
			} finally {
				if (out != null) {
					try {
						out.close();
					} catch (IOException e) {
						logger.error("Exception happened when close "
								+ propertyFile, e);
					}
				}
			}
		}
	}

	/*
	 * Save cachedQueries to property File
	 */
	public void close() {
		saveCachedQueries();
	}

	private static final String getPropertyFilePath(SolrCore core) {
		return core.getDataDir() + File.separator + PROPERTY_FILE;
	}

	public NamedList<Object> remove(String query) {
		CacheKeyHashMap params = convertQueryStringToParams(query, null);
		synchronized (cachedQueries) {
			cachedQueries.remove(query);
		}
		synchronized (map) {
			return remove(params);
		}
	}

	public NamedList<Object> remove(CacheKeyHashMap key) {
		synchronized (map) {
			return map.remove(key);
		}
	}

	public NamedList<Object> put(String query, NamedList<Object> value,
			boolean persist) {
		return put(query, value, persist, true, null);
	}

	public NamedList<Object> put(String query, NamedList<Object> value,
			boolean persist, boolean addDefault) {
		return put(query, value, persist, addDefault, null);
	}

	public NamedList<Object> put(String query, NamedList<Object> value,
			boolean persist, boolean addDefault, String handlerName) {
		CacheKeyHashMap key = convertQueryStringToParams(query, handlerName);
		if (persist) {
			synchronized (cachedQueries) {
				cachedQueries.add(query);
			}
		}
		return put(key, value);
	}

	@Override
	public NamedList<Object> put(CacheKeyHashMap key, NamedList<Object> value) {
		if (value != null) {
			value.remove("CachedAt");
			value.add("CachedAt",
					DateUtil.getThreadLocalDateFormat().format(new Date()));
		}
		return super.put(key, value);
	}

	public void asyncRefill() {
		refill(false);
	}

	public void refill(boolean sync) {
		if (sync) {
			refillImpl();
		} else {
			new Thread(new Runnable() {
				@Override
				public void run() {
					refillImpl();
				}
			}).start();
		}
	}

	@SuppressWarnings("unchecked")
	private void refillImpl() {
		synchronized (map) {
			SolrQueryRequest myreq = null;
			try {
				Iterator<CacheKeyHashMap> it = map.keySet().iterator();
				SolrRequestHandler searchHandler = core
						.getRequestHandler("/select");

				Map<CacheKeyHashMap, NamedList<Object>> newValue = new HashMap<CacheKeyHashMap, NamedList<Object>>();
				myreq = new LocalSolrQueryRequest(core,
						new ModifiableSolrParams());
				while (it.hasNext()) {
					CacheKeyHashMap query = it.next();
					SolrQueryResponse rsp = new SolrQueryResponse();
					searchHandler.handleRequest(myreq, rsp);
					MultiMapSolrParams params = new MultiMapSolrParams(query);
					myreq.setParams(params);
					newValue.put(query, rsp.getValues());
				}
				map.putAll(newValue);
			} finally {
				if (myreq != null) {
					myreq.close();
				}
			}
		}
	}

	public void clearValues() {
		synchronized (map) {
			Iterator<Map.Entry<CacheKeyHashMap, NamedList<Object>>> it = map
					.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<CacheKeyHashMap, NamedList<Object>> entry = it.next();
				entry.setValue(null);
			}
		}
	}

	public static CacheKeyHashMap getKey(SolrQueryRequest req) {
		ModifiableSolrParams modifiableParams = new ModifiableSolrParams(
				req.getParams());
		modifiableParams.remove("cache");
		modifiableParams.remove("refresh");
		return paramsToHashMap(modifiableParams);
	}

	@Override
	public void warm(SolrIndexSearcher searcher,
			SolrCache<CacheKeyHashMap, NamedList<Object>> old) {
		throw new UnsupportedOperationException();
	}

	// ////////////////////// SolrInfoMBeans methods //////////////////////

	private static CacheKeyHashMap paramsToHashMap(
			ModifiableSolrParams modifiableParams) {
		CacheKeyHashMap map = new CacheKeyHashMap();
		map.putAll(SolrParams.toMultiMap(modifiableParams.toNamedList()));
		return map;
	}

	@SuppressWarnings("rawtypes")
	private CacheKeyHashMap convertQueryStringToParams(String query,
			String handlerName) {
		ModifiableSolrParams modifiableParams = new ModifiableSolrParams();
		if (handlerName == null) {
			handlerName = "/select";
		}
		RequestHandlerBase handler = (RequestHandlerBase) core
				.getRequestHandler(handlerName);
		NamedList initArgs = handler.getInitArgs();
		if (initArgs != null) {
			Object o = initArgs.get("defaults");
			if (o != null && o instanceof NamedList) {
				modifiableParams.add(SolrParams.toSolrParams((NamedList) o));
			}
			o = initArgs.get("appends");
			if (o != null && o instanceof NamedList) {
				modifiableParams.add(SolrParams.toSolrParams((NamedList) o));
			}
			o = initArgs.get("invariants");
			if (o != null && o instanceof NamedList) {
				modifiableParams.add(SolrParams.toSolrParams((NamedList) o));
			}
		}
		modifiableParams.add(SolrRequestParsers.parseQueryString(query));
		return paramsToHashMap(modifiableParams);
	}

	@Override
	public String getName() {
		return QueryResultLRUCache.class.getName();
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public String getSource() {
		return "$URL: https://svn.apache.org/repos/asf/lucene/dev/branches/branch_4x/solr/core/src/java/org/apache/solr/search/QueryResultLRUCache.java $";
	}

}

class CacheKeyHashMap extends HashMap<String, String[]> {
	private static final long serialVersionUID = 1L;

	public int hashCode() {
		int h = 0;
		Iterator<Entry<String, String[]>> i = entrySet().iterator();
		while (i.hasNext()) {
			Entry<String, String[]> entry = i.next();
			h += (entry.getKey() == null ? 0 : entry.getKey().hashCode())
					^ (entry.getValue() == null ? 0 : Arrays.hashCode(entry
							.getValue()));
		}
		return h;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof Map))
			return false;
		Map<String, String[]> m = (Map<String, String[]>) o;
		if (m.size() != size())
			return false;

		try {
			Iterator<Entry<String, String[]>> i = entrySet().iterator();
			while (i.hasNext()) {
				Entry<String, String[]> e = i.next();
				String key = e.getKey();
				String[] value = e.getValue();
				if (value == null) {
					if (!(m.get(key) == null && m.containsKey(key)))
						return false;
				} else {
					if (!Arrays.equals(value, m.get(key)))
						return false;
				}
			}
		} catch (ClassCastException unused) {
			return false;
		} catch (NullPointerException unused) {
			return false;
		}

		return true;
	}

	@Override
	public String toString() {
		return super.toString();
	}
}

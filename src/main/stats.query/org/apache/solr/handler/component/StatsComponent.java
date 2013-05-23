/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.component;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.UnInvertedField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrQueryParser;

/**
 * Stats component calculates simple statistics on numeric field values
 * @since solr 1.4
 */
public class StatsComponent extends SearchComponent {

  public static final String STATS_PAGINATION = "stats.pagination";
  public static final String STATS_QUERY_OVERALLSTATS = "stats.query.overallstats";
  public static final String STATS_QUERIES = "stats_queries";
  public static final String DEFAULT_SORT_FIELD = "sum";
  public static final long DEFAULT_FACET_LIMIT = 50L;

  public static final String COMPONENT_NAME = "stats";
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (rb.req.getParams().getBool(StatsParams.STATS,false)) {
      rb.setNeedDocSet( true );
      rb.doStats = true;
      handleStatsQueryParams(rb);
    }
  }

  /**
   * Post condition: size of queries and labesList should be same.
   */
  private void handleStatsQueryParams(ResponseBuilder rb) {
    SolrParams params = rb.req.getParams();
    // key is stats.field, value the a pair of query and label list for this tats field
    Map<String,Pair<List<String>, List<String>>> statsQueriesMap = new HashMap<String,Pair<List<String>, List<String>>>();
    rb.setStatsQueriesMap(statsQueriesMap);
    String[] statsFs = params.getParams(StatsParams.STATS_FIELD);
    boolean pagination = params.getBool(StatsComponent.STATS_PAGINATION, false);
    if (null != statsFs) {
      for (String fieldName : statsFs) {
        List<String> queries = new ArrayList<String>(), labeList = new ArrayList<String>();
        if (pagination) {
          // convert ranges to queries
          List<String> rangeQuries = handleRangeParmas(rb, fieldName);
          for (String query : rangeQuries) {
            queries.add(query);
            labeList.add(query);
          }
          // handle stats.query
          handleStatsQueryParams(rb, fieldName, queries, labeList);
          statsQueriesMap.put(fieldName, new Pair<List<String>,List<String>>(queries, labeList));
        }
      }
    }
  }
  

  /**
   * @param statsField
   * @return a list of query strings
   */
  private List<String> handleRangeParmas(ResponseBuilder rb, final String statsField) {
      SolrParams params = rb.req.getParams();
      String startStr = params.getFieldParam(statsField, "stats.range.start");
      String endStr = params.getFieldParam(statsField, "stats.range.end");
      String gapStr = params.getFieldParam(statsField, "stats.range.gap");
      String rangesStr = params.getFieldParam(statsField, "stats.ranges");

      Double start = null, end = null;
      if (startStr != null) {
          start = Double.valueOf(startStr);
      }
      if (endStr != null) {
          end = Double.valueOf(endStr);
      }

      final List<String> queries = new ArrayList<String>();
      List<String> ranges = new ArrayList<String>();
      String fieldName = statsField;
      if (rangesStr != null) {
          int index = rangesStr.indexOf(':');
          if (index > 0) {
              fieldName = rangesStr.substring(0, index);
          }
          ranges = StrUtils.splitSmart(rangesStr.substring(index + 1), ',');
      } else if (gapStr != null) {
          List<String> strs = StrUtils.splitSmart(gapStr, ',');
          double[] gap = new double[strs.size()];
          for (int i = 0; i < strs.size(); i++) {
              gap[i] = Double.parseDouble(strs.get(i));
          }
          if (start != null && end != null & gap != null) {
              ranges = getRanges(rb, statsField, start, end, gap);
          }
      }

      for (int i = 0; i < ranges.size() - 1; i++) {
          queries.add(fieldName + ":[" + ranges.get(i).trim() + " TO "
                  + ranges.get(i + 1) + "}");
      }
      return queries;
  }
  
  private List<String> getRanges(ResponseBuilder rb, String field,
      double start, double end, double[] gap) {
    List<Double> ranges = new ArrayList<Double>();
    if (gap == null || gap.length == 0) {
      ranges.add(start);
      ranges.add(end);
    } else {
      double boundEnd = start;
      int i = 0;
      int lastIndex = gap.length - 1;
      while (boundEnd <= end) {
        ranges.add(boundEnd);
        if (lastIndex < i) {
          boundEnd += gap[lastIndex];
        } else {
          boundEnd += gap[i];
        }
        i++;
      }
      if (ranges.get(ranges.size() - 1) < end) {
        ranges.add(end);
      }
    }
    List<String> result = new ArrayList<String>();
    
    FieldType fieldType = rb.req.getSearcher().getSchema().getField(field)
        .getType();
    String calssName = fieldType.getClass().getName();
    if (calssName.contains("LongField") || calssName.contains("IntField")
        || calssName.contains("ShortField")) {
      for (int i = 0; i < ranges.size(); i++) {
        result.add(String.valueOf(ranges.get(i).longValue()));
      }
    } else {
      for (int i = 0; i < ranges.size(); i++) {
        result.add(ranges.get(i).toString());
      }
    }
    return result;
  }
  
  private void handleStatsQueryParams(ResponseBuilder rb,
      final String filedName, List<String> queries, List<String> labesList) {
    SolrParams params = rb.req.getParams();
    String[] queriesStr = params.getFieldParams(filedName, "stats.query");
    if (queriesStr != null) {
      // handle query label: example:stats.query={!label="Label Name"}qstr
      for (int i = 0; i < queriesStr.length; i++) {
        String oldQuery = queriesStr[i].trim();
        if (oldQuery.startsWith("{!")) {
          int endIndex = oldQuery.indexOf('}');
          if (endIndex < 0) {
            throw new RuntimeException("Local parameter is not correct: "
                + oldQuery);
          }
          String label = queriesStr[i].substring(2, endIndex).trim();
          if (label.startsWith("label=")) {
            label = label.substring(6).trim();
            if (label.startsWith("\"") || label.startsWith("'")) {
              label = label.substring(1);
              if (label.endsWith("\"") || label.endsWith("'")) {
                label = label.substring(0, label.length() - 1);
              } else {
                label = null;
              }
            } else {
              label = null;
            }
          } else {
            label = null;
          }
          String newQuery = oldQuery.substring(endIndex + 1).trim();
          queries.add(newQuery);
          if (label != null) {
            labesList.add(label);
          } else {
            labesList.add(newQuery);
          }
        } else {
          queries.add(oldQuery);
          labesList.add(oldQuery);
        }
      }
    }
  }
  
  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (rb.doStats) {
      SolrParams params = rb.req.getParams();
      SimpleStats s = new SimpleStats(rb.req,
              rb.getResults().docSet,
              params, rb.getStatsQueriesMap() );

      // TODO ???? add this directly to the response, or to the builder?
      rb.rsp.add( "stats", s.getStatsCounts() );
    }
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!rb.doStats) return;

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      sreq.purpose |= ShardRequest.PURPOSE_GET_STATS;
      SolrParams params = rb.req.getParams();
      StatsInfo si = rb._statsInfo;
      if (si == null) {
        rb._statsInfo = si = new StatsInfo();
        si.parse(params, rb);
        // should already be true...
        // sreq.params.set(StatsParams.STATS, "true");
      }
      // change stats.facet.offset and stats.facet.limit
      String[] statsFs = params.getParams(StatsParams.STATS_FIELD);
      if(statsFs==null) return;
      for (String statsField : statsFs) {
        String[] facets = params.getFieldParams(statsField,
            StatsParams.STATS_FACET);
        if(facets==null) break;
        for (String facet : facets) {
          sreq.params.remove("stats.facet." + facet + ".offset");
          sreq.params.set("f." + statsField + ".stats.facet." + facet
              + ".offset", 0);
          
          Long limit = Long.MAX_VALUE;
          // hidden feature, in case exact mode doesn't work, maybe due to too many facts in one solr server.
          boolean exact = true;
          String str = params.getFieldParam(statsField, "stats.facet." + facet + ".exact");
          if (str != null) {
            exact = Boolean.parseBoolean(str);
          }
          if (!exact) {
            str = params.getFieldParam(statsField, "stats.facet." + facet
                + ".limit");
            if (str != null) {
              limit = Long.valueOf(str);
              limit = limit * 2;
            }
          }
          
          sreq.params.remove("stats.facet." + facet + ".limit");
          sreq.params.set("f." + statsField + ".stats.facet." + facet
              + ".limit", String.valueOf(limit));
        }
      }
      // use javabin to reduce the size of response from one server
      sreq.params.set("wt", "javabin");
    } else {
      // turn off stats on other requests
      sreq.params.set(StatsParams.STATS, "false");
      // we could optionally remove stats params
    }
  }

  @SuppressWarnings({"rawtypes"})
  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (!rb.doStats || (sreq.purpose & ShardRequest.PURPOSE_GET_STATS) == 0) return;

    StatsInfo si = rb._statsInfo;
    for (ShardResponse srsp : sreq.responses) {
      NamedList stats = (NamedList) srsp.getSolrResponse().getResponse()
          .get("stats");
      
      NamedList stats_fields = (NamedList) stats.get("stats_fields");
      if (stats_fields != null) {
        for (int i = 0; i < stats_fields.size(); i++) {
          String field = stats_fields.getName(i);
          StatsValues stv = si.statsFields.get(field);
          NamedList shardStv = (NamedList) stats_fields.get(field);
          stv.accumulate(shardStv);
        }
      }
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public void finishStage(ResponseBuilder rb) {
    if (!rb.doStats || rb.stage != ResponseBuilder.STAGE_GET_FIELDS) return;
    // wait until STAGE_GET_FIELDS
    // so that "result" is already stored in the response (for aesthetics)

    StatsInfo si = rb._statsInfo;

    NamedList<NamedList<Object>> stats = new SimpleOrderedMap<NamedList<Object>>();
    NamedList<Object> stats_fields = new SimpleOrderedMap<Object>();
    stats.add("stats_fields", stats_fields);
    SolrParams params = rb.req.getParams();

    boolean extension = Boolean.parseBoolean(params.get(STATS_PAGINATION));
    if (extension) {
      Map<String,Pair<List<String>, List<String>>> statsQueriesMap = rb.getStatsQueriesMap();
      String[] statsFs = params.getParams(StatsParams.STATS_FIELD);
      if (statsFs == null) return;
      boolean overStats = params.getBool(
          StatsComponent.STATS_QUERY_OVERALLSTATS, false);
      
      for (String statsField : statsFs) {
        
        NamedList stasStv = si.statsFields.get(statsField).getStatsValues();
        if (stasStv == null) {
          continue;
        }
        NamedList nl = (NamedList) stasStv.get(STATS_QUERIES);
        if (nl == null) {
          continue;
        }
        if (!overStats) {
          // remove the overallStats for this stats field
          Iterator<Map.Entry<String,Object>> it = stasStv.iterator();
          while (it.hasNext()) {
            Entry<String,Object> entry = it.next();
            String key = entry.getKey();
            if (!STATS_QUERIES.equals(key)) {
              stasStv.remove(key);
            }
          }
        }
        handleFacet(params, statsField, nl);
        
        // make the order of stats.query same as request
        NamedList sortedNL = new NamedList();
        Pair<List<String>, List<String>> pair = statsQueriesMap.get(statsField);
        if(pair!=null)
        {
          List<String> labels = pair.getB();
          if(labels!=null)
          {
            for (String label : labels) {
              NamedList<Object> queryValue = (NamedList<Object>) nl.get(label);
              if (queryValue != null) {
                Object obj = queryValue.get("count");
                if (obj != null && (Long) obj == 0) {
                  queryValue = new NamedList<Object>();
                }
              }
              sortedNL.add(label, queryValue == null ? new NamedList<Object>() : queryValue);
            }
          }
         int index = stasStv.indexOf(STATS_QUERIES, 0);
         stasStv.setVal(index, sortedNL);
        }

        stats_fields.add(statsField, stasStv);
      }
    }
    else {
      for (String field : si.statsFields.keySet()) {
        NamedList stv = si.statsFields.get(field).getStatsValues();
        Object obj = stv.get("count");
        if (obj == null || (Long) stv.get("count") != 0) {
          stats_fields.add(field, stv);
        } else {
          stats_fields.add(field, null);
        }
      }
    }

    rb.rsp.add("stats", stats);
    rb._statsInfo = null;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private String[] handleFacet(SolrParams params, String statsField,
      NamedList nl) {
    String[] facets = params.getFieldParams(statsField,
        StatsParams.STATS_FACET);
    // iterate each stats.field
    if (facets != null) {
      for (String facetField : facets) {
        long offset = 0;
        String sortBy = DEFAULT_SORT_FIELD;
        String str = params.getFieldParam(statsField, "stats.facet."
            + facetField + ".offset");
        if (str != null) {
          offset = Long.parseLong(str);
        }
        
        long limit = StatsComponent.DEFAULT_FACET_LIMIT;
        str = params.getFieldParam(statsField, "stats.facet." + facetField
            + ".limit");
        if (str != null) {
          limit = Long.parseLong(str);
        }
        str = params.getFieldParam(statsField, "stats.facet." + facetField
            + ".sortfield");
        if (str != null) {
          sortBy = str;
        }
        
        Iterator<Map.Entry<String,NamedList<NamedList<Object>>>> iterator = nl
            .iterator();
        // iterate each stats.query for the stats.field
        while (iterator.hasNext()) {
          Map.Entry<String,NamedList<NamedList<Object>>> entry = iterator
              .next();
          NamedList<NamedList<Object>> queryValue = entry.getValue();
          
          NamedList<Object> fieldFacets = queryValue.get("facets");
          if (fieldFacets == null) {
            continue;
          }
          NamedList<NamedList<Object>> fieldFacetList = (NamedList<NamedList<Object>>) fieldFacets
              .get(facetField);
          NamedList<NamedList<Object>> facetResult = SimpleStats
              .handleFacetsOffsetLimitSortBy(fieldFacetList, offset, limit,
                  sortBy);
          
          NamedList<Object> facetNL = new NamedList<Object>();
          // the real totalCount
          facetNL.add("totalCount", fieldFacetList.size());
          facetNL.add("offset", offset);
          facetNL.add("limit", limit);
          facetNL.addAll((NamedList) facetResult);
          fieldFacets.remove(facetField);
          fieldFacets.add(facetField, facetNL);
        }
      }
    }
    return facets;
  }
  
  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Calculate Statistics";
  }

  @Override
  public String getSource() {
    return "$URL: https://svn.apache.org/repos/asf/lucene/dev/branches/branch_4x/solr/core/src/java/org/apache/solr/handler/component/StatsComponent.java $";
  }

}
class Pair<A, B> {

  private final A a;
  private final B b;

  public Pair(A a, B b) {
    this.a = a;
    this.b = b;
  }

  public A getA() {
    return a;
  }

  public B getB() {
    return b;
  }
}
class StatsInfo {
  Map<String, StatsValues> statsFields;

  void parse(SolrParams params, ResponseBuilder rb) {
    statsFields = new HashMap<String, StatsValues>();

    String[] statsFs = params.getParams(StatsParams.STATS_FIELD);
    if (statsFs != null) {
      for (String field : statsFs) {
        SchemaField sf = rb.req.getSchema().getField(field);
        statsFields.put(field, StatsValuesFactory.createStatsValues(sf));
      }
    }
  }
}

class SimpleStats {

  /** The main set of documents */
  protected DocSet docs;
  /** Configuration params behavior should be driven by */
  protected SolrParams params;
  /** Searcher to use for all calculations */
  protected SolrIndexSearcher searcher;
  protected SolrQueryRequest req;
  protected Map<String,Pair<List<String>,List<String>>> statsQueriesMap;
  public SimpleStats(SolrQueryRequest req,
                      DocSet docs,
                      SolrParams params) {
    this(req, docs, params, new HashMap<String,Pair<List<String>,List<String>>>());
  }

  public SimpleStats(SolrQueryRequest req, DocSet docs, SolrParams params,
      Map<String,Pair<List<String>,List<String>>> statsQueriesMap) {
    this.req = req;
    this.searcher = req.getSearcher();
    this.docs = docs;
    this.params = params;
    this.statsQueriesMap = statsQueriesMap;
  }

  public NamedList<Object> getStatsCounts() throws IOException {
    NamedList<Object> res = new SimpleOrderedMap<Object>();
    res.add("stats_fields", getStatsFields());
    return res;
  }

  @SuppressWarnings("unchecked")
	public NamedList<Object> getStatsFields() throws IOException {
    
    NamedList<Object> res = new SimpleOrderedMap<Object>();
    String[] statsFs = params.getParams(StatsParams.STATS_FIELD);
    boolean isShard = params.getBool(ShardParams.IS_SHARD, false);
    boolean pagination = params.getBool(StatsComponent.STATS_PAGINATION, false);
    if (null != statsFs) {
  		for (String fieldName : statsFs) {
  			String[] facets = params.getFieldParams(fieldName,
  					StatsParams.STATS_FACET);
  			if (facets == null) {
  				facets = new String[0]; // make sure it is something...
  			}

  			SchemaField sf = searcher.getSchema().getField(fieldName);
  			FieldType ft = sf.getType();
        List<String> queries = new ArrayList<String>(), labeList = new ArrayList<String>();
  			String[] facetSortFields = new String[facets.length];
  			Long[] offsets = new Long[facets.length];
  			Long[] limits = new Long[facets.length];
        if (pagination) {
          // handleQueryParams(fieldName, queries, labeList);
          Pair<List<String>,List<String>> pair = statsQueriesMap.get(fieldName);
          if (pair.getA() != null) {
            queries = pair.getA();
          }
          if (pair.getB() != null) {
            labeList = pair.getB();
          }
          
          parseSortFieldsAndTop(fieldName, facets, facetSortFields, offsets,
              limits);
        }
        
  			// Currently, only UnInvertedField can deal with multi-part trie
  			// fields
  			String prefix = TrieField.getMainValuePrefix(ft);
  			if (sf.multiValued() || ft.multiValuedFieldCache()
  					|| prefix != null || !queries.isEmpty()) {
  				UnInvertedField uif = UnInvertedField.getUnInvertedField(
  						fieldName, searcher);

  				if (queries.isEmpty()) {
  				  NamedList<Object>	facetResult = doGetStatsField(uif, docs, null, isShard,
  							fieldName, facets, facetSortFields, offsets, limits, pagination);

  					res.add(facetResult.getName(0), facetResult.getVal(0));
          } else {
            // add overall stats for stats.query
            boolean overStats = params.getBool(StatsComponent.STATS_QUERY_OVERALLSTATS,
                false);
            
            NamedList<Object> statsQueryNL = new NamedList<Object>();
            handleMultipleQueries(uif, fieldName, queries, labeList, facets,
                facetSortFields, offsets, limits, isShard, pagination,
                statsQueryNL);
            
            if (overStats == true) {
              NamedList<Object> includeOverallNL = new NamedList<Object>();
              
              NamedList<Object> overStatsNL = (NamedList<Object>) uif.getStats(
                  searcher, docs, new String[0]).getStatsValues();
              Object obj = overStatsNL.get("count");
              if (obj != null && (Long) obj > 0) {
                includeOverallNL.addAll(overStatsNL);
                includeOverallNL.addAll(statsQueryNL);
                res.add(fieldName, includeOverallNL);
              }
              else
              {
                res.add(fieldName, statsQueryNL);
              }
              
            } else {
              res.add(fieldName, statsQueryNL);
            }
          }
  			} else {
  				NamedList<Object> stv = (NamedList<Object>) getFieldCacheStats(
  						fieldName, facets);
  				filterFacetResponse(facets, facetSortFields, offsets, limits, stv, pagination);

          Object obj = stv.get("count");
  				if (isShard || (obj != null && (Long) obj > 0)) {
  					res.add(fieldName, stv);
  				} else {
            // res.add(fieldName, null);
            res.add(fieldName, new NamedList<Object>());
  				}
  			}
  		}
  	}
    return res;
  }
  
  // why does this use a top-level field cache?
  public NamedList<?> getFieldCacheStats(String fieldName, String[] facet) throws IOException {
    final SchemaField sf = searcher.getSchema().getField(fieldName);

    final StatsValues allstats = StatsValuesFactory.createStatsValues(sf);

    List<FieldFacetStats> facetStats = new ArrayList<FieldFacetStats>();
    for( String facetField : facet ) {
      SchemaField fsf = searcher.getSchema().getField(facetField);

      if ( fsf.multiValued()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Stats can only facet on single-valued fields, not: " + facetField );
      }

      facetStats.add(new FieldFacetStats(searcher, facetField, sf, fsf));
    }

    final Iterator<AtomicReaderContext> ctxIt = searcher.getIndexReader().leaves().iterator();
    AtomicReaderContext ctx = null;
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc()) {
        // advance
        do {
          ctx = ctxIt.next();
        } while (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc());
        assert doc >= ctx.docBase;

        // propagate the context among accumulators.
        allstats.setNextReader(ctx);
        for (FieldFacetStats f : facetStats) {
          f.setNextReader(ctx);
        }
      }

      // accumulate
      allstats.accumulate(doc - ctx.docBase);
      for (FieldFacetStats f : facetStats) {
        f.facet(doc - ctx.docBase);
      }
    }

    for (FieldFacetStats f : facetStats) {
      allstats.addFacet(f.name, f.facetStatsValues);
    }
    return allstats.getStatsValues();
  }

  private void parseSortFieldsAndTop(String fieldName, String[] facets,
      String[] facetSortFields, Long[] offsets, Long[] limits) {
    for (int i = 0; i < facets.length; i++) {
      String facet = facets[i];
      String offset = params.getFieldParam(fieldName, "stats.facet." + facet
          + ".offset");
      if (offset != null) {
        long tmp = Long.parseLong(offset.trim());
        if(tmp<0) throw new IllegalArgumentException("Value of " + "stats.facet." + facet
            + ".offset must be greater than or equal to zero.");
        offsets[i] = tmp;
      } 
      String limit = params.getFieldParam(fieldName, "stats.facet." + facet
          + ".limit");
      if (limit != null) {
        long tmp = Long.parseLong(limit.trim());
        if(tmp<= 0) throw new IllegalArgumentException("Value of " + "stats.facet." + facet
            + ".limit must be greater than zero.");
        limits[i] = tmp;
      }
      
      String facetSortField = params.getFieldParam(fieldName, "stats.facet."
          + facet + ".sortfield");
      if (facetSortField != null) {
        facetSortField = facetSortField.trim();
        if (facetSortField.equals("")) {
          throw new IllegalArgumentException("Paramter stats.facet." + facet
              + ".sortfield" + " can't be emptry");
        }
        facetSortFields[i] = facetSortField.trim();
      }
      else
      {
        facetSortFields[i] = StatsComponent.DEFAULT_SORT_FIELD;
      }
    }
  }

  private void handleMultipleQueries(final UnInvertedField uif,
      final String filedName, final List<String> queries,
      List<String> labeList, final String[] facets,
      final String[] facetSortFields, final Long[] offsets,
      final Long[] limits, final boolean isShard, final boolean pagination,
      NamedList<Object> statsQueryNL) {
    final List<NamedList<Object>> tmpResult = new ArrayList<NamedList<Object>>();
    for (int i = 0; i < queries.size(); i++) {
      tmpResult.add(null);
    }
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < queries.size(); i++) {
      final int index = i;
      Thread thread = new Thread() {
        @Override
        public void run() {
          NamedList<Object> tmpFacetResult = null;
          String tmpQuery = queries.get(index);
          try {
            QParser qparser = QParser.getParser(tmpQuery, "lucene", req);
            SolrQueryParser parser = new SolrQueryParser(qparser, req
                .getSchema().getDefaultSearchFieldName());
            Query query = parser.parse(tmpQuery);
            DocSet docSet = searcher.getDocSet(query);
            DocSet interSection = docs.intersection(docSet);
            tmpFacetResult = doGetStatsField(uif, interSection, tmpQuery,
                isShard, filedName, facets, facetSortFields, offsets, limits,
                pagination);
            
          } catch (Exception e) {
            tmpFacetResult = new NamedList<Object>();
            
            NamedList<Object> error = new NamedList<Object>();
            NamedList<Object> value = new NamedList<Object>();
            value.add("msg", e.getMessage());
            StringWriter stacks = new StringWriter();
            e.printStackTrace(new PrintWriter(stacks));
            value.add("trace", stacks.toString());
            
            error.add("error", value);
            tmpFacetResult.add(tmpQuery, error);
            throw new RuntimeException(e);
          } finally {
            tmpResult.set(index, tmpFacetResult);
          }
        }
      };
      thread.start();
      threads.add(thread);
      
    }
    
    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    
    NamedList<Object> queriesNL = new NamedList<Object>();
    
    for (int i = 0; i < tmpResult.size(); i++) {
      NamedList<Object> namedList = tmpResult.get(i);
      if (namedList != null && namedList.size() > 0) {
        // namedList should only have one result, maybe empty
        if (labeList.get(i) != null) {
          queriesNL.add(labeList.get(i), namedList.getVal(0));
        } else {
          queriesNL.add(namedList.getName(0), namedList.getVal(0));
        }
      }
      else
      {
        if (labeList.get(i) != null) {
          queriesNL.add(labeList.get(i), new NamedList<Object>());
        } else {
          queriesNL.add(namedList.getName(0), new NamedList<Object>());
        }
      }
    }
    
    statsQueryNL.add(StatsComponent.STATS_QUERIES, queriesNL);
  }

//    /**
//     * Post condition: size of queries and labesList should be same.
//     */
//    private void handleQueryParams(final String filedName,
//            List<String> queries, List<String> labesList) {
//        // convert ranges to queries
//        List<String> rangeQuries = handleRangeParmas(filedName);
//        for (String query : rangeQuries) {
//            queries.add(query);
//            labesList.add(null);
//        }
//        // handle stats.query
//        handleStatsQueryParams(filedName, queries, labesList);
//    }

//    private void handleStatsQueryParams(final String filedName,
//            List<String> queries, List<String> labesList) {
//        String[] queriesStr = params.getFieldParams(filedName, "stats.query");
//        if (queriesStr != null) {
//            // handle query label: example:stats.query={!label="Label Name"}qstr
//            for (int i = 0; i < queriesStr.length; i++) {
//                String oldQuery = queriesStr[i].trim();
//                if (oldQuery.startsWith("{!")) {
//                    int endIndex = oldQuery.indexOf('}');
//                    if (endIndex < 0) {
//                        throw new RuntimeException(
//                                "Local parameter is not correct: " + oldQuery);
//                    }
//                    String label = queriesStr[i].substring(2, endIndex).trim();
//                    if (label.startsWith("label=")) {
//                        label = label.substring(6).trim();
//                        if (label.startsWith("\"") || label.startsWith("'")) {
//                            label = label.substring(1);
//                        }
//                        if (label.endsWith("\"") || label.endsWith("'")) {
//                            label = label.substring(0, label.length() - 1);
//                        }
//                        labesList.add(label);
//                    } else {
//                        labesList.add(null);
//                    }
//                    queries.add(oldQuery.substring(endIndex + 1).trim());
//                } else {
//                    queries.add(oldQuery);
//                    labesList.add(null);
//                }
//            }
//        }
//    }

//    private List<String> getRanges(String field, double start, double end,
//            double[] gap) {
//        List<Double> ranges = new ArrayList<Double>();
//        if (gap == null || gap.length == 0) {
//            ranges.add(start);
//            ranges.add(end);
//        } else {
//            double boundEnd = start;
//            int i = 0;
//            int lastIndex = gap.length - 1;
//            while (boundEnd <= end) {
//                ranges.add(boundEnd);
//                if (lastIndex < i) {
//                    boundEnd += gap[lastIndex];
//                } else {
//                    boundEnd += gap[i];
//                }
//                i++;
//            }
//            if (ranges.get(ranges.size() - 1) < end) {
//                ranges.add(end);
//            }
//        }
//        List<String> result = new ArrayList<String>();
//
//        FieldType fieldType = searcher.getSchema().getField(field).getType();
//        String calssName = fieldType.getClass().getName();
//        if (calssName.contains("LongField") || calssName.contains("IntField")
//                || calssName.contains("ShortField")) {
//            for (int i = 0; i < ranges.size(); i++) {
//                result.add(String.valueOf(ranges.get(i).longValue()));
//            }
//        } else {
//            for (int i = 0; i < ranges.size(); i++) {
//                result.add(ranges.get(i).toString());
//            }
//        }
//        return result;
//    }

    /**
     * @param statsField
     * @return a list of query strings
     */
//    private List<String> handleRangeParmas(final String statsField) {
//        String startStr = params.getFieldParam(statsField, "stats.range.start");
//        String endStr = params.getFieldParam(statsField, "stats.range.end");
//        String gapStr = params.getFieldParam(statsField, "stats.range.gap");
//        String rangesStr = params.getFieldParam(statsField, "stats.ranges");
//
//        Double start = null, end = null;
//        if (startStr != null) {
//            start = Double.valueOf(startStr);
//        }
//        if (endStr != null) {
//            end = Double.valueOf(endStr);
//        }
//
//        final List<String> queries = new ArrayList<String>();
//        List<String> ranges = new ArrayList<String>();
//        String fieldName = statsField;
//        if (rangesStr != null) {
//            int index = rangesStr.indexOf(':');
//            if (index > 0) {
//                fieldName = rangesStr.substring(0, index);
//            }
//            ranges = StrUtils.splitSmart(rangesStr.substring(index + 1), ',');
//        } else if (gapStr != null) {
//            List<String> strs = StrUtils.splitSmart(gapStr, ',');
//            double[] gap = new double[strs.size()];
//            for (int i = 0; i < strs.size(); i++) {
//                gap[i] = Double.parseDouble(strs.get(i));
//            }
//            if (start != null && end != null & gap != null) {
//                ranges = getRanges(statsField, start, end, gap);
//            }
//        }
//
//        for (int i = 0; i < ranges.size() - 1; i++) {
//            queries.add(fieldName + ":[" + ranges.get(i).trim() + " TO "
//                    + ranges.get(i + 1) + "}");
//        }
//        return queries;
//    }

    /**
     * Call UnInvertedField to get stats and handle sortfields and topn.
     * 
     * @return
     */
  @SuppressWarnings("unchecked")
  private NamedList<Object> doGetStatsField(UnInvertedField uif, DocSet docs,
      String lable, boolean isShard, String fieldName, String[] facets,
      String[] facetSortFields, Long[] offsets, Long[] limits, boolean pagination) {
    try {
      NamedList<Object> res = new NamedList<Object>();
      NamedList<Object> stv = (NamedList<Object>) uif.getStats(searcher, docs,
          facets).getStatsValues();
      filterFacetResponse(facets, facetSortFields, offsets, limits, stv, pagination);
      Object obj = stv.get("count");
      if (isShard || (obj != null && (Long) obj > 0)) {
        res.add(lable == null ? fieldName : lable, stv);
      } else {
        res.add(lable == null ? fieldName : lable, new NamedList<Object>());
      }
      return res;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void filterFacetResponse(String[] facets, String[] facetSortFields,
      Long[] offsets, Long[] limits, NamedList<Object> stv, boolean pagination) {
    if(!pagination) return;
    
    NamedList<NamedList<NamedList<Object>>> fieldFacet = (NamedList<NamedList<NamedList<Object>>>) (stv
        .get("facets"));
    NamedList<Object> filteredFacets = new NamedList<Object>();
    if(fieldFacet ==null) return;
    
    for (int j = 0; j < facets.length; j++) {
      String sortBy = facetSortFields[j];
      NamedList<NamedList<Object>> fieldFacetList = fieldFacet.get(facets[j]);
      // safe check only, should not happen
      if (fieldFacetList == null) {
        continue;
      }
      NamedList<Object> result = new NamedList<Object>();
      result.add("totalCount", fieldFacetList.size());
      
      Long offset = offsets[j];
      Long limit = limits[j];
      if (sortBy == null) sortBy = StatsComponent.DEFAULT_SORT_FIELD;
      if (offset == null) offset = 0L;
      // if limit is not provided, set it as 50. 
      if (limit == null) limit = StatsComponent.DEFAULT_FACET_LIMIT;
      
      result.add("offset", offset);
      result.add("limit", limit);
      NamedList<NamedList<Object>> namedList = doFilterFacetResponse(fieldFacetList,
          offset, limit, sortBy);
      result.addAll((NamedList) namedList);
      filteredFacets.add(facets[j], result);
    }
    if (filteredFacets.size() > 0) {
      stv.remove("facets");
      stv.add("facets", filteredFacets);
    }
  }
  
  private NamedList<NamedList<Object>> doFilterFacetResponse(
      NamedList<NamedList<Object>> fieldFacetList, long offset, long limit,
      String sortBy) {
    // No need to sort the facets.
    if (sortBy == null) {
      Iterator<Map.Entry<String,NamedList<Object>>> facetIt = fieldFacetList
          .iterator();
       // skip offset
       while (facetIt.hasNext()) {
         --offset;
         if (offset < 0) break;
         facetIt.next();
       }
       NamedList<NamedList<Object>> result = new NamedList<NamedList<Object>>();
      while (facetIt.hasNext()) {
        Map.Entry<String,NamedList<Object>> entry = facetIt.next();
        result.add(entry.getKey(), entry.getValue());
        --limit;
        if (limit <= 0) break;
      }
      return result;
    } else {
      return  handleFacetsOffsetLimitSortBy(fieldFacetList, offset, limit, sortBy);
    }
  }

  static NamedList<NamedList<Object>> handleFacetsOffsetLimitSortBy(
      NamedList<NamedList<Object>> fieldFacetList, long offset, long limit,
      String sortBy) {
    NamedList<NamedList<Object>> result = new NamedList<NamedList<Object>>();

    Iterator<Map.Entry<String,NamedList<Object>>> facetIt = fieldFacetList
        .iterator();
    long treeSetSize =  offset+limit;
    TreeSetTopnAscendingByValue<String,Double> topnSet = new TreeSetTopnAscendingByValue<String,Double>(treeSetSize);
    while (facetIt.hasNext()) {
      Map.Entry<String,NamedList<Object>> entry = facetIt.next();
      String newLabel = entry.getKey();
      NamedList<?> namedList = entry.getValue();
      double newValue = Double.parseDouble(namedList.get(sortBy).toString());
      topnSet.add(newLabel, newValue);
    }
    
    Iterator<Entry<String,Double>> topnIterator = topnSet.descendingIterator();
    
    // skip offset
    while (topnIterator.hasNext()) {
      --offset;
      if (offset < 0) break;
      topnIterator.next();
    }
   while (topnIterator.hasNext()) {
      Entry<String,Double> entry = topnIterator.next();
      String label = entry.getKey();
      result.add(label, fieldFacetList.get(label));
    }
   
   return result;
  }

  static class TreeSetTopnAscendingByValue<K extends Comparable<? super K>, V extends Comparable<? super V>>
      extends TreeSet<Map.Entry<K, V>> {
    private static final long serialVersionUID = 1L;
    private long maxSize = Long.MAX_VALUE;
    private Comparator<Map.Entry<K, V>> comparator;

    public TreeSetTopnAscendingByValue(Comparator<Map.Entry<K, V>> comparator,
        long maxSize) {
      super(comparator);
      this.comparator = comparator;
      if(maxSize<=0) throw new IllegalArgumentException("Argument maxSize must be greater than 0, exact value: " + maxSize);
      this.maxSize = maxSize;
    }

    public TreeSetTopnAscendingByValue(long maxSize) {
      this(new Comparator<Map.Entry<K, V>>() {
        public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
          int res = e1.getValue().compareTo(e2.getValue());
          if (res == 0) {
            res = e1.getKey().compareTo(e2.getKey());
          }
          return res;
        }
      }, maxSize);
    }

    public boolean add(K newKey, V newValue) {
      return add(new SimpleEntry<K, V>(newKey, newValue));
    }

    @Override
    public boolean add(Entry<K, V> newValue) {
      boolean added = false;
      if (this.size() > maxSize)
        throw new RuntimeException("Should never happen.");
      if (this.size() == maxSize) {
        Iterator<Entry<K, V>> iterator = this.iterator();
        Entry<K, V> currentMin = iterator.next();
        // only remove currentMin if newValue > currentMin. if equals, do nothing
        // for better performance.
        if (comparator.compare(newValue, currentMin) > 0) {
          added = super.add(newValue);
          // the added element may already exist, if the map doesn't have this
          // element(added is true), remove currentMin
          if (added) {
            iterator = this.iterator();
            iterator.next();
            iterator.remove();
          }
        } 
      } else {
        added = super.add(newValue);
      }
      return added;
    }
  }
}
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class MultiCoreUpdateRequestHandler extends UpdateRequestHandler {
  private static String PARAM_SHARDS = "shards";
  
  @Override
  public void handleRequestBody(final SolrQueryRequest req,
      final SolrQueryResponse rsp) throws Exception {
    try {
      
      SolrParams params = req.getParams();
      String shardsStr = params.get(PARAM_SHARDS);
      if (shardsStr == null) {
        throw new RuntimeException("No shards paramter found.");
      }
      List<String> shards = StrUtils.splitSmart(shardsStr, ',');
      
      List<ModifiableSolrParams> shardParams = new ArrayList<ModifiableSolrParams>();
      for (int i = 0; i < shards.size(); i++) {
        shardParams.add(new ModifiableSolrParams());
      }
      Iterator<String> iterator = params.getParameterNamesIterator();
      String shardParamPrefix = "shard";
      while (iterator.hasNext()) {
        String paramName = iterator.next();
        if (paramName.equals(PARAM_SHARDS)) continue;
        if (paramName.startsWith(shardParamPrefix)) {
          int index = paramName.indexOf(".");
          if (index < 0) continue;
          String numStr = paramName.substring(shardParamPrefix.length(), index);
          try {
            int shardNumber = Integer.parseInt(numStr);
            String shardParam = paramName.substring(index + 1);
            shardParams.get(shardNumber).add(shardParam, params.get(paramName));
          } catch (Exception e) {
            // do nothing
          }
        } else {
          // add common parameters
          for (ModifiableSolrParams tmp : shardParams) {
            tmp.add(paramName, params.get(paramName));
          }
        }
      }
      handleShards(shards, shardParams, rsp);
    } finally {}
  }
  
  private void handleShards(final List<String> shards,
      final List<ModifiableSolrParams> shardParams, final SolrQueryResponse rsp)
      throws InterruptedException {
    
    ExecutorService executor = null;
    
    executor = Executors.newFixedThreadPool(shards.size());
    
    for (int i = 0; i < shards.size(); i++) {
      final int index = i;
      executor.submit(new Runnable() {
        @SuppressWarnings("unchecked")
        @Override
        public void run() {
          Map<String,Object> resultMap = new LinkedHashMap<String,Object>();
          try {
            SolrServer solr = new HttpSolrServer(shards.get(index));
            
            ModifiableSolrParams params = shardParams.get(index);
            UpdateRequest request = new UpdateRequest(params.get("url"));
            resultMap.put("params", params.toNamedList());
            request.setParams(params);
            UpdateResponse response = request.process(solr);
            NamedList<Object> header = response.getResponseHeader();
            resultMap.put("responseHeader", header);
            System.err.println(response);
          } catch (Exception e) {
            NamedList<Object> error = new NamedList<Object>();
            error.add("msg", e.getMessage());
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            error.add("trace", sw.toString());
            resultMap.put("error", error);
            throw new RuntimeException(e);
          } finally {
            rsp.add("shard" + index, resultMap);
          }
        }
      });
    }
    executor.shutdown();
    
    boolean terminated = executor.awaitTermination(Long.MAX_VALUE,
        TimeUnit.SECONDS);
    if (!terminated) {
      throw new RuntimeException("Request takes too much time");
    }
  }
}

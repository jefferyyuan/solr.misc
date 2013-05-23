/**
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;

public class MultiSearchHandler extends SearchHandler {
  
  private List<SearchComponent> queryComponent = null;
  
  protected List<String> getDefaultComponents() {
    return super.getDefaultComponents();
  }
  
  private void splitComponents() {
    queryComponent = new ArrayList<SearchComponent>();
    queryComponent.add(components.get(0));
  }
  
  /**
   * Initialize the components based on name. Note, if using
   * {@link #INIT_FIRST_COMPONENTS} or {@link #INIT_LAST_COMPONENTS}, then the
   * {@link DebugComponent} will always occur last. If this is not desired, then
   * one must explicitly declare all components using the
   * {@link #INIT_COMPONENTS} syntax.
   */
  public void inform(SolrCore core) {
    super.inform(core);
    this.splitComponents();
  }
  
  private int initRequestParams(SolrQueryRequest req,
      Vector<SimpleOrderedMap<Object>> localRequestParams,
      SimpleOrderedMap<Object> commonRequestParams) {
    SolrParams reqParams = req.getParams();
    Iterator<String> iter = null;
    int count = -1;
    
    try {
      count = Integer.parseInt(reqParams.get("count"));
    } catch (NumberFormatException ex) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Could not find count parameter");
    }
    
    for (int i = 0; i < count; i++) {
      localRequestParams.add(new SimpleOrderedMap<Object>());
    }
    
    iter = reqParams.getParameterNamesIterator();
    
    while (iter.hasNext()) {
      String paramName = (String) iter.next();
      int queryId = 0;
      
      StringTokenizer token = new StringTokenizer(paramName, ".");
      try {
        queryId = Integer.parseInt(token.nextToken());
        int startPos = paramName.indexOf('.') + 1;
        
        String[] paramsValues = reqParams.getParams(paramName);
        if(paramsValues!=null)
        {
          for(String value: paramsValues)
          {
            localRequestParams.elementAt(queryId - 1).add(
                paramName.substring(startPos),value);
          }
        }
//        localRequestParams.elementAt(queryId - 1).add(
//            // Changed by yy:  reqParams.get(paramName)
//            paramName.substring(startPos), reqParams.get(paramName));
      } catch (NumberFormatException ex) {
        String[] paramVals = reqParams.getParams(paramName);
        for (String paramValue : paramVals)
          commonRequestParams.add(paramName, paramValue);
        continue;
      } catch (SolrException ex) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Invalid parameter. Please check url parameters associated with the request.");
        
      } catch (IndexOutOfBoundsException ex) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Invalid request parameters.  Please check url parameters associated with the request");
      }
    }
    return count;
  }
  
  private Vector<LocalSolrQueryRequest> createLocalRequests(SolrCore solrCore,
      Vector<SimpleOrderedMap<Object>> localRequestParams,
      SimpleOrderedMap<Object> commonRequestParams) {
    Vector<LocalSolrQueryRequest> localRequests = new Vector<LocalSolrQueryRequest>();
    int count = localRequestParams.size();
    for (int i = 0; i < count; i++) {
      localRequestParams.get(i).addAll(commonRequestParams);
      localRequests.add(new LocalSolrQueryRequest(solrCore, localRequestParams
          .elementAt(i)));
    }
    return localRequests;
  }
  
  private int getNumFound(SolrQueryResponse rsp, int responseNumber) {
    NamedList<Object> Response = rsp.getValues();
    List<Object> dsl = Response.getAll("response");
    ResultContext dslTmp = (ResultContext) dsl.get(responseNumber);
    return dslTmp.docs.matches();
  }
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception {
    int defThreshold = -1, i = 0;
    Integer[] thresholds = null;
    Boolean[] passesThresholds = null;
    Boolean stopOnPass = false;
    Boolean skipOnFailure = false;
    
    ResponseBuilder rb = null;
    Vector<LocalSolrQueryRequest> localRequests = null;
    Vector<SimpleOrderedMap<Object>> localRequestParams = new Vector<SimpleOrderedMap<Object>>();
    SimpleOrderedMap<Object> commonRequestParams = new SimpleOrderedMap<Object>();
    
    SolrParams reqParams = req.getParams();
    SolrCore solrCore = req.getCore();
    
    // default threshold for all the queries
    String tmp = reqParams.get("threshold");
    if (tmp != null) defThreshold = Integer.parseInt(tmp);
    tmp = reqParams.get("stoponpass");
    if (tmp != null) stopOnPass = Boolean.parseBoolean(tmp);
    tmp = reqParams.get("skiponfailure");
    if (tmp != null) skipOnFailure = Boolean.parseBoolean(tmp);
    
    int count = this.initRequestParams(req, localRequestParams,
        commonRequestParams);
    
    try {
      localRequests = this.createLocalRequests(solrCore, localRequestParams,
          commonRequestParams);
      
      thresholds = new Integer[count];
      passesThresholds = new Boolean[count];
      
      /* Parse Thresholds and Skip On Failures */
      for (i = 0; i < count; i++) {
        String threshold_tmp = reqParams.get((i + 1) + ".threshold");
        if (threshold_tmp != null) thresholds[i] = Integer
            .parseInt(threshold_tmp);
        else thresholds[i] = defThreshold;
      }
      
      /*
       * Find out the queries that have passed the threshold when when we dont
       * have to send the response on failure or when we need to stop on first
       * query whose numFound>threshold
       */
      if (skipOnFailure == true || stopOnPass == true) {
        for (i = 0; i < count; i++) {
          rb = new ResponseBuilder(localRequests.get(i), rsp, queryComponent);
          handleRequestBody(localRequests.get(i), rsp, queryComponent, rb);
          int size = getNumFound(rsp, 0);
          if (size >= thresholds[i]) passesThresholds[i] = true;
          else passesThresholds[i] = false;
          ((NamedList<Object>) rsp.getValues()).remove("response");
          if (stopOnPass && passesThresholds[i]) break;
        }
      } else {
        for (i = 0; i < count; i++) {
          passesThresholds[i] = true;
        }
      }
      
      for (i = 0; i < count; i++) {
        if (passesThresholds[i] != null
            && (passesThresholds[i] || !skipOnFailure)) {
          rb = new ResponseBuilder(localRequests.get(i), rsp, components);
          handleRequestBody(localRequests.get(i), rsp, components, rb);
        }
      }
    } finally {
      for (i = 0; i < localRequests.size(); i++) {
        LocalSolrQueryRequest lsh = localRequests.get(i);
        lsh.close();
      }
    }
  }
  
  // ////////////////////// SolrInfoMBeans methods //////////////////////
  @Override
  public String getVersion() {
    return "$Revision$";
  }
  
  @Override
  public String getSource() {
    return "$URL$";
  }
  
}
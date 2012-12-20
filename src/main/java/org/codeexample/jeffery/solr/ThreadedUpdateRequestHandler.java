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

package org.codeexample.jeffery.solr;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;

/**
 * UpdateHandler that uses content-type to pick the right Loader
 */
public class ThreadedUpdateRequestHandler extends UpdateRequestHandler {

	private static String PARAM_THREAD_NUMBER = "threads";

	private static String PARAM_STREAM_FOLDER = "stream.folder";
	private static String PARAM_STREAM_FILE_PATTERN = "stream.file.pattern";

	private static final int DEFAULT_THREAD_NUMBER = 10;
	private static int DEFAULT_THREADS = DEFAULT_THREAD_NUMBER;

	@SuppressWarnings("rawtypes")
	@Override
	public void init(NamedList args) {
		super.init(args);
		if (args != null) {
			NamedList namedList = ((NamedList) args.get("defaults"));
			if (namedList != null) {
				Object obj = namedList.get(PARAM_THREAD_NUMBER);
				if (obj != null) {
					DEFAULT_THREADS = Integer.parseInt(obj.toString());
				}
			}
		}
	}

	@Override
	public void handleRequestBody(final SolrQueryRequest req,
			final SolrQueryResponse rsp) throws Exception {

		List<ContentStream> streams = new ArrayList<ContentStream>();

		handleReqStream(req, streams);
		// here, we handle the new two parameters: stream.folder and
		// strem.filepattern
		handleStreamFolders(req, streams);
		handleFilePatterns(req, streams);
		if (streams.size() < 2) {
			// No need to use threadpool.
			SolrQueryRequestBase reqBase = (SolrQueryRequestBase) req;
			if (!streams.isEmpty()) {
				String contentType = req.getParams().get(
						CommonParams.STREAM_CONTENTTYPE);
				ContentStream stream = streams.get(0);
				if (stream instanceof ContentStreamBase) {
					((ContentStreamBase) stream).setContentType(contentType);

				}
			}
			reqBase.setContentStreams(streams);
			super.handleRequestBody(req, rsp);
		} else {
			importStreamsMultiThreaded(req, rsp, streams);
		}
	}

	private void handleReqStream(final SolrQueryRequest req,
			List<ContentStream> streams) {
		Iterable<ContentStream> iterabler = req.getContentStreams();
		if (iterabler != null) {
			Iterator<ContentStream> iterator = iterabler.iterator();
			while (iterator.hasNext()) {
				streams.add(iterator.next());
				iterator.remove();
			}
		}
	}

	private ExecutorService importStreamsMultiThreaded(
			final SolrQueryRequest req, final SolrQueryResponse rsp,
			List<ContentStream> streams) throws InterruptedException,
			IOException {
		ExecutorService executor = null;
		SolrParams params = req.getParams();

		final UpdateRequestProcessorChain processorChain = req
				.getCore()
				.getUpdateProcessingChain(params.get(UpdateParams.UPDATE_CHAIN));

		UpdateRequestProcessor processor = processorChain.createProcessor(req,
				rsp);
		try {
			Map<String, Object> resultMap = new LinkedHashMap<String, Object>();

			resultMap.put("start_time", new Date());
			List<Map<String, Object>> details = new ArrayList<Map<String, Object>>();

			try {

				int threads = determineThreadsNumber(params, streams.size());
				ThreadFactory threadFactory = new ThreadFactory() {
					public Thread newThread(Runnable r) {
						return new Thread(r, "threadedReqeustHandler-"
								+ new Date());
					}
				};
				executor = Executors.newFixedThreadPool(threads, threadFactory);
				String contentType = params
						.get(CommonParams.STREAM_CONTENTTYPE);

				Iterator<ContentStream> iterator = streams.iterator();
				while (iterator.hasNext()) {
					ContentStream stream = iterator.next();
					iterator.remove();
					if (stream instanceof ContentStreamBase) {
						((ContentStreamBase) stream)
								.setContentType(contentType);

					}
					submitTask(req, rsp, processorChain, executor, stream,
							details);
				}

				executor.shutdown();

				boolean terminated = executor.awaitTermination(Long.MAX_VALUE,
						TimeUnit.SECONDS);
				if (!terminated) {
					throw new RuntimeException("Request takes too much time");
				}
				// Perhaps commit from the parameters
				RequestHandlerUtils.handleCommit(req, processor, params, false);
				RequestHandlerUtils.handleRollback(req, processor, params,
						false);
			} finally {
				resultMap.put("end_time", new Date());

				// check whether there is error in details
				for (Map<String, Object> map : details) {
					Exception ex = (Exception) map.get("exception");
					if (ex != null) {
						rsp.setException(ex);
						if (ex instanceof SolrException) {
							rsp.add("status", ((SolrException) ex).code());
						} else {
							rsp.add("status",
									SolrException.ErrorCode.BAD_REQUEST);
						}
						break;
					}
				}
			}
			resultMap.put("details", details);
			rsp.add("result", resultMap);
			return executor;
		} finally {
			if (executor != null && !executor.isShutdown()) {
				executor.shutdownNow();
			}
			// finish the request
			processor.finish();
		}
	}

	private int determineThreadsNumber(SolrParams params, int streamSize) {
		int threads = DEFAULT_THREADS;
		String str = params.get(PARAM_THREAD_NUMBER);
		if (str != null) {
			threads = Integer.parseInt(str);
		}

		if (streamSize < threads) {
			threads = streamSize;
		}
		return threads;
	}

	private void handleFilePatterns(final SolrQueryRequest req,
			List<ContentStream> streams) {
		String[] strs = req.getParams().getParams(PARAM_STREAM_FILE_PATTERN);
		if (strs != null) {
			for (String filePattern : strs) {
				// it may point to a file
				File file = new File(filePattern);
				if (file.isFile()) {
					streams.add(new ContentStreamBase.FileStream(file));
				} else {
					// only supports tail regular expression, such as
					// c:\foldera\c*.csv
					int lastIndex = filePattern.lastIndexOf(File.separator);
					if (lastIndex > -1) {
						File folder = new File(filePattern.substring(0,
								lastIndex));

						if (!folder.exists()) {
							throw new RuntimeException("Folder " + folder
									+ " doesn't exists.");
						}

						String pattern = filePattern.substring(lastIndex + 1);
						pattern = convertPattern(pattern);
						final Pattern p = Pattern.compile(pattern);

						File[] files = folder.listFiles(new FilenameFilter() {
							@Override
							public boolean accept(File dir, String name) {
								Matcher matcher = p.matcher(name);
								return matcher.matches();
							}
						});

						if (files != null) {
							for (File tmp : files) {
								streams.add(new ContentStreamBase.FileStream(
										tmp));
							}
						}
					}
				}
			}
		}
	}

	private void handleStreamFolders(final SolrQueryRequest req,
			List<ContentStream> streams) {
		String[] strs = req.getParams().getParams(PARAM_STREAM_FOLDER);
		if (strs != null) {
			for (String folderStr : strs) {

				File folder = new File(folderStr);

				File[] files = folder.listFiles();

				if (files != null) {
					for (File file : files) {
						streams.add(new ContentStreamBase.FileStream(file));
					}
				}
			}
		}
	}

	/**
	 * replace * to .*, replace . to \.
	 */
	private String convertPattern(String pattern) {
		pattern = pattern.replaceAll("\\.", "\\\\.");
		pattern = pattern.replaceAll("\\*", ".*");
		return pattern;
	}

	private void submitTask(final SolrQueryRequest req,
			final SolrQueryResponse rsp,
			final UpdateRequestProcessorChain processorChain,
			ExecutorService executor, final ContentStream stream,
			final List<Map<String, Object>> rspResult) {
		Thread thread = new Thread() {
			public void run() {
				Map<String, Object> map = new LinkedHashMap<String, Object>();
				map.put("start_time", new Date().toString());

				if (stream instanceof ContentStreamBase.FileStream) {
					map.put("Import File: ",
							((ContentStreamBase.FileStream) stream).getName());
				}
				try {
					UpdateRequestProcessor processor = null;
					try {
						processor = processorChain.createProcessor(req, rsp);

						ContentStreamLoader documentLoader = newLoader(req,
								processor);

						documentLoader.load(req, rsp, stream, processor);
						System.err.println(rsp);

					} finally {
						if (processor != null) {
							// finish the request
							processor.finish();
						}
					}
				} catch (Exception e) {
					rsp.setException(e);
				} finally {
					map.put("end_time", new Date().toString());
					if (rsp.getException() != null) {
						map.put("exception", rsp.getException());
					}
					rspResult.add(map);
				}

			};
		};

		executor.execute(thread);
	}
}

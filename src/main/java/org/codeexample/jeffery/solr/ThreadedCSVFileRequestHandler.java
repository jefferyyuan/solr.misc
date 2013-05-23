package org.codeexample.jeffery.solr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.ContentStreamBase.FileStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;

public class ThreadedCSVFileRequestHandler extends UpdateRequestHandler {
  private static final Logger logger = LoggerFactory.getLogger(ThreadedCSVFileRequestHandler.class);

  private static final String PARAM_THREADED_CSV_FILE = "threaded_csv_file";

  public static final String PARAM_THREAD_POOL_SIZE = "threaded_csv_file_thread_pool_size";
  public static final String PARAM_QUEUE_SIZE = "threaded_csv_file_queue_size";

  private static final String PARAM_USE_CONFIGURED_FIELD_NAMES = "useconfiguredfieldnames";

  private static final String PARAM_LINES_READ_FILE = "lines_per_file";
  private static final String PARAM_FILE_SIZE_LIMIT_MB = "file_limit_mb";
  private static final String PARAM_SPLIT_FILE_NUMBER_LIMIT = "split_file_number_limit";

  private static final String PARAM_FIELD_NAMES = "fieldnames";
  private String fieldnames;

  private boolean threadedcsvfile = false;
  private int threadPoolSize;
  private int threadPoolQueueSize;
  // default 100 million
  private int linesPerFile;
  // unit mb
  private int defaultFileSizeLimitMB;
  private static final long MB = 1024 * 1024;

  private int splitFileNumberLimit;

  @SuppressWarnings("rawtypes")
  @Override
  public void init(NamedList args) {
    super.init(args);
    if (args != null) {
      SolrParams params = SolrParams.toSolrParams(args);
      threadedcsvfile = params.getBool(PARAM_THREADED_CSV_FILE, false);
      threadPoolSize = params.getInt(PARAM_THREAD_POOL_SIZE, 1000);
      threadPoolQueueSize = params.getInt(PARAM_QUEUE_SIZE, 1000);
      fieldnames = params.get(PARAM_FIELD_NAMES);

      linesPerFile = params.getInt(PARAM_LINES_READ_FILE, 1000000);
      defaultFileSizeLimitMB = params.getInt(PARAM_FILE_SIZE_LIMIT_MB, 200);
      splitFileNumberLimit = params.getInt(PARAM_SPLIT_FILE_NUMBER_LIMIT, 50);
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();

    boolean useconfiguredfieldnames = true;
    boolean tmpThreadedcsvfile = threadedcsvfile;
    if (params != null) {
      useconfiguredfieldnames = params.getBool(PARAM_USE_CONFIGURED_FIELD_NAMES, true);
      tmpThreadedcsvfile = params.getBool(PARAM_THREADED_CSV_FILE, threadedcsvfile);
    }

    if (useconfiguredfieldnames) {
      ModifiableSolrParams modifiableSolrParams = new ModifiableSolrParams(params);
      modifiableSolrParams.set(PARAM_FIELD_NAMES, fieldnames);
      req.setParams(modifiableSolrParams);
    }

    if (tmpThreadedcsvfile) {
      List<ContentStream> streams = getStreams(req);
      if (streams.size() > splitFileNumberLimit) {
        super.handleRequestBody(req, rsp);
      } else {
        threadedCSVFile(req, streams);
      }
    } else {
      super.handleRequestBody(req, rsp);
    }
  }

  private void threadedCSVFile(SolrQueryRequest req, List<ContentStream> streams) throws IOException,
      InterruptedException {
    ThreadCSVFilePoolExecutor importExecutor = null;
    ThreadCSVFilePoolExecutor submitFileExecutor = null;
    final List<File> tmpDirs = new ArrayList<File>();

    try {
      if (req instanceof SolrQueryRequestBase) {
        SolrQueryRequestBase requestBase = (SolrQueryRequestBase) req;
        requestBase.setContentStreams(null);
      }
      List<ContentStream> otherStreams = new ArrayList<ContentStream>(streams.size());

      List<ContentStreamBase.FileStream> streamFiles = new ArrayList<ContentStreamBase.FileStream>(streams.size());

      Iterator<ContentStream> iterator = streams.iterator();
      while (iterator.hasNext()) {
        ContentStream stream = iterator.next();
        iterator.remove();
        if (stream instanceof ContentStreamBase.FileStream) {
          streamFiles.add((FileStream) stream);
        } else {
          otherStreams.add(stream);
        }
      }

      importExecutor = newExecutor(threadPoolSize, threadPoolQueueSize);

      iterator = otherStreams.iterator();
      while (iterator.hasNext()) {
        List<ContentStream> tmpStreams = new ArrayList<ContentStream>();
        tmpStreams.add(iterator.next());
        iterator.remove();

        ImportCSVDataTask callable = new ImportCSVDataTask(this, req, new SolrQueryResponse(), tmpStreams);
        importExecutor.submit(callable);
      }

      Throwable throwable = importExecutor.getTaskThrows();
      if (throwable != null) {
        // should already be shutdown
        logger.error(this.getClass().getName() + " throws exception, shutdown threadpool now.", throwable);
        importExecutor.shutdownNow();
        throw new RuntimeException(throwable);
      }
      if (!streamFiles.isEmpty()) {

        long fileLimit = getFileLimitMB(req);
        // now handle csv files
        Iterator<ContentStreamBase.FileStream> fileStreamIt = streamFiles.iterator();
        // List<Thread> threads = new ArrayList<Thread>(streamFiles.size());

        submitFileExecutor = newExecutor(threadPoolSize, threadPoolQueueSize);

        while (fileStreamIt.hasNext()) {
          ContentStreamBase.FileStream fileStream = fileStreamIt.next();
          fileStreamIt.remove();

          if (fileStream.getSize() <= fileLimit) {
            List<ContentStream> tmpStreams = new ArrayList<ContentStream>();
            tmpStreams.add(fileStream);
            ImportCSVDataTask callable = new ImportCSVDataTask(this, req, new SolrQueryResponse(), tmpStreams);
            importExecutor.submit(callable);
          } else {
            SubmitCSVFileTask task = new SubmitCSVFileTask(importExecutor, this, req, fileStream, linesPerFile, tmpDirs);
            submitFileExecutor.submit(task);
          }
        }
        throwable = submitFileExecutor.getTaskThrows();
      }
      if (throwable != null) {
        // should already be shutdown
        importExecutor.shutdownNow();
        if (submitFileExecutor != null) {
          submitFileExecutor.shutdownNow();
        }
        logger.error(this.getClass().getName() + " throws exception, shutdown threadpool now.", throwable);
        throw new RuntimeException(throwable);
      }
      boolean terminated = false;
      if (submitFileExecutor != null) {
        submitFileExecutor.shutdown();
        terminated = submitFileExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        if (!terminated) {
          logger.error("shutdown submitFileExecutor takes too much time");
          throw new RuntimeException("Request takes too much time");
        }
      }
      importExecutor.shutdown();
      terminated = importExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
      if (!terminated) {
        logger.error("shutdown importExecutor takes too much time");
        throw new RuntimeException("Request takes too much time");
      }
    } finally {
      if (submitFileExecutor != null) {
        try {
          submitFileExecutor.shutdownNow();
        } catch (Exception e) {
          logger.error("submitFileExecutor.shutdownNow throws: " + e);
        }
      }
      if (importExecutor != null) {
        try {
          importExecutor.shutdownNow();
        } catch (Exception e) {
          logger.error("importExecutor.shutdownNow throws: " + e);
        }
      }

      // remove all files in tmpDirs
      new Thread() {
        public void run() {
          for (File dir : tmpDirs) {
            try {
              deleteDirectory(dir);
              logger.info("Deleted tmp dir:" + dir);
            } catch (IOException e) {
              logger.error("Exception happened when delete tmp dir: " + dir, e);
            }
          }
        };
      }.start();
    }

  }

  void deleteDirectory(File dir) throws IOException {
    if (dir.isDirectory()) {
      for (File file : dir.listFiles()) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          file.delete();
        }
      }
    }
    if (!dir.delete())
      throw new FileNotFoundException("Failed to delete file: " + dir);
  }

  private long getFileLimitMB(SolrQueryRequest req) {
    long mb = req.getParams().getInt(PARAM_FILE_SIZE_LIMIT_MB, defaultFileSizeLimitMB);
    return mb * MB;
  }

  private static class SubmitCSVFileTask implements Callable<Void> {
    // private volatile boolean running = true;
    private ContentStreamBase.FileStream fileStream;
    private ThreadCSVFilePoolExecutor executor;
    private ThreadedCSVFileRequestHandler requestHandler;

    private SolrQueryRequest req;
    private BufferedReader srcBr;
    private int linesPerFile;
    private List<File> tmpDirs;

    public SubmitCSVFileTask(ThreadCSVFilePoolExecutor executor, ThreadedCSVFileRequestHandler requestHandler,
        SolrQueryRequest req, FileStream fileStream, int lines, List<File> tmpDirs) throws IOException {
      super();
      this.executor = executor;
      this.requestHandler = requestHandler;
      this.req = req;
      this.fileStream = fileStream;
      srcBr = new BufferedReader(fileStream.getReader());
      this.linesPerFile = lines;
      this.tmpDirs = tmpDirs;
    }

    private void doSplitSubmit(File srcFile, File tmpDir) throws Exception {
      logger.info("Start to split " + srcFile + " to " + tmpDir);
      int counter = 0;
      try {
        while (srcBr.ready()) {
          String newFileName = tmpDir.getAbsolutePath() + File.separator + srcFile.getName() + counter;
          File newFile = new File(newFileName);
          ++counter;
          boolean created = copyTo(newFile);
          if (!created) {
            break;
          } else {
            // submit file
            FileStream tmpFileStream = new FileStream(newFile);
            List<ContentStream> tmpStreams = new ArrayList<ContentStream>();
            tmpStreams.add(tmpFileStream);
            ImportCSVDataTask callable = new ImportCSVDataTask(requestHandler, req, new SolrQueryResponse(), tmpStreams);
            executor.submit(callable);
          }
        }
      } finally {
        if (srcBr != null) {
          try {
            srcBr.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
      logger.info("Finished split " + srcFile + " to " + tmpDir);
    }

    private boolean copyTo(File newFile) throws Exception {
      boolean created = true;
      int linesRead = 0;
      BufferedWriter bw = new BufferedWriter(new FileWriter(newFile));
      try {
        while (linesRead < linesPerFile) {
          String line = srcBr.readLine();
          if (line == null) {
            break;
          } else {
            line = line.trim();
            if (line.length() != 0) {
              ++linesRead;
              bw.write(line);
              bw.newLine();
            }
          }
        }
      } finally {
        if (bw != null) {
          try {
            bw.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        if (linesRead == 0) {
          newFile.delete();
          created = false;
        }
      }
      return created;
    }

    @Override
    public Void call() throws Exception {
      try {
        URI uri = URI.create(fileStream.getSourceInfo());
        File srcFile = new File(uri);
        File tmpDir = createTempDir(srcFile.getName());
        tmpDirs.add(tmpDir);
        if (logger.isDebugEnabled()) {
          logger.debug("Create tmpdir: " + tmpDir.getAbsolutePath());
        }
        doSplitSubmit(srcFile, tmpDir);
      } catch (Exception e) {
        logger.error("Exception happened when handle file: " + fileStream.getName(), e);
        throw e;
      } finally {
        if (srcBr != null) {
          try {
            srcBr.close();
          } catch (IOException e) {
            logger.error("Exception happened when close BufferedReader for file: " + fileStream.getName(), e);

          }
        }
      }
      return null;
    }

    public static File createTempDir(String prefix) {
      File baseDir = new File(System.getProperty("java.io.tmpdir"));
      String baseName = System.currentTimeMillis() + "-";
      int TEMP_DIR_ATTEMPTS = 20;
      for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
        File tempDir = new File(baseDir, prefix + "-" + baseName + counter);
        if (tempDir.mkdir()) {
          return tempDir;
        }
      }
      throw new IllegalStateException("Failed to create directory within " + TEMP_DIR_ATTEMPTS + " attempts (tried "
          + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
    }
  }

  /**
   *
   */
  private static class ImportCSVDataTask implements Callable<Void> {
    private SolrQueryRequest req;
    private SolrQueryResponse rsp;
    private ThreadedCSVFileRequestHandler requestHandler;
    private List<ContentStream> streams;

    // private long endLine;

    public ImportCSVDataTask(ThreadedCSVFileRequestHandler requestHandler, SolrQueryRequest req, SolrQueryResponse rsp,
        List<ContentStream> streams) {
      super();
      this.req = req;
      this.rsp = rsp;
      this.requestHandler = requestHandler;
      this.streams = streams;
    }

    @Override
    public Void call() throws Exception {
      UpdateRequestProcessor processor = null;

      UpdateRequestProcessorChain processorChain = req.getCore().getUpdateProcessingChain(
          req.getParams().get(UpdateParams.UPDATE_CHAIN));

      processor = processorChain.createProcessor(req, rsp);
      ContentStreamLoader documentLoader = requestHandler.newLoader(req, processor);

      for (ContentStream stream : streams) {
        if (stream.getName() != null) {
          logger.info("Start to import " + stream.getName());
        }
        documentLoader.load(req, rsp, stream, processor);
        if (stream.getName() != null) {
          logger.info("Finished import " + stream.getName());
        }
      }

      return null;
    }
  }

  private ThreadCSVFilePoolExecutor newExecutor(int threadPoolSize, int queueSize) {
    ThreadCSVFilePoolExecutor executor;
    executor = new ThreadCSVFilePoolExecutor(threadPoolSize, threadPoolSize, 60, TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(queueSize), new ThreadPoolExecutor.CallerRunsPolicy());
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  private static class ThreadCSVFilePoolExecutor extends ThreadPoolExecutor {

    private Throwable taskThrows;

    public Throwable getTaskThrows() {
      return taskThrows;
    }

    public ThreadCSVFilePoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        ArrayBlockingQueue<Runnable> workQueue, CallerRunsPolicy callerRunsPolicy) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, new CSVFileThreadedThreadFactory(
          ThreadedCSVFileRequestHandler.class.getName()), callerRunsPolicy);
    }

    /*
     * From:
     * http://stackoverflow.com/questions/2248131/handling-exceptions-from-java
     * -executorservice-tasks
     */
    @Override
    protected void afterExecute(Runnable runnable, Throwable throwable) {
      super.afterExecute(runnable, throwable);
      taskThrows = throwable;
      if (throwable == null && runnable instanceof Future<?>) {
        try {
          Future<?> future = (Future<?>) runnable;
          future.get();
        } catch (CancellationException ce) {
          taskThrows = ce;
        } catch (ExecutionException ee) {
          taskThrows = ee.getCause();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt(); // ignore/reset
        }
      }
      if (taskThrows != null) {
        logger.error("Task throws exception, shutdown threadpool." + taskThrows);
        shutdownNow();
      }
    }

  }

  private static class CSVFileThreadedThreadFactory implements ThreadFactory {
    private static final AtomicInteger POOLNUMBER = new AtomicInteger(1);
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private String namePrefix;

    CSVFileThreadedThreadFactory(String prefix) {
      SecurityManager s = System.getSecurityManager();
      group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
      namePrefix = (prefix == null ? this.getClass().getSimpleName() : prefix) + POOLNUMBER.getAndIncrement()
          + "-thread-";
    }

    public Thread newThread(Runnable r) {
      Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
      if (t.isDaemon())
        t.setDaemon(false);
      if (t.getPriority() != Thread.NORM_PRIORITY)
        t.setPriority(Thread.NORM_PRIORITY);
      return t;
    }
  }
}

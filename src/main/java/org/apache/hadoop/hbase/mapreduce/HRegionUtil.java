/* Copyright 2012 Explorys, Inc                                                                                                                                                                                                                
 *                                                                                                                                                                                                                                          
 *     Licensed under the Apache License, Version 2.0 (the "License");                                                                                                                                                                      
 *   you may not use this file except in compliance with the License.                                                                                                                                                                       
 *   You may obtain a copy of the License at                                                                                                                                                                                                
 *                                                                                                                                                                                                                                          
 *       http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                                                                         
 *                                                                                                                                                                                                                                          
 *       Unless required by applicable law or agreed to in writing, software                                                                                                                                                                
 *       distributed under the License is distributed on an "AS IS" BASIS,                                                                                                                                                                  
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.                                                                                                                                                           
 *   See the License for the specific language governing permissions and                                                                                                                                                                    
 *   limitations under the License.                                                                                                                                                                                                         
 */
package org.apache.hadoop.hbase.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Stack;

import javax.naming.OperationNotSupportedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HFileResultInputFormat;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.NoWritesHRegion;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

public class HRegionUtil {
    
    
    public static final String HLOG_WRITER_IMPLEMENTATION_CLASS = "hbase.regionserver.hlog.writer.impl";
    public static final String HBASE_RELATIVE_DIR = "/hbase/";
    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table  The table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @throws IOException When setting up the details fails.
     */
    //We don't really care the generic parameters to TableMapper or WritableComparable
    @SuppressWarnings("rawtypes")
    public static void initTableMapperJob(String table, Scan scan,
        Class<? extends TableMapper> mapper,
        Class<? extends WritableComparable> outputKeyClass,
        Class<? extends Writable> outputValueClass, Job job)
    throws IOException {
      initTableMapperJob(table, scan, mapper, outputKeyClass, outputValueClass,
          job, true);
    }

    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table  The table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @param addDependencyJars upload HBase jars and jars for any of the configured
     *           job classes via the distributed cache (tmpjars).
     * @throws IOException When setting up the details fails.
     */
    //We don't really care the generic parameters to TableMapper of WritableComparable
    @SuppressWarnings("rawtypes")
    public static void initTableMapperJob(String table, Scan scan,
         Class<? extends TableMapper> mapper,
        Class<? extends WritableComparable> outputKeyClass,
        Class<? extends Writable> outputValueClass, Job job,
        boolean addDependencyJars)
    throws IOException {
      job.setInputFormatClass(HFileResultInputFormat.class);
      if (outputValueClass != null) job.setMapOutputValueClass(outputValueClass);
      if (outputKeyClass != null) job.setMapOutputKeyClass(outputKeyClass);
      job.setMapperClass(mapper);
      HBaseConfiguration.addHbaseResources(job.getConfiguration());
      job.getConfiguration().set(HFileResultInputFormat.TABLE_NAME_PARAM, table);
      job.getConfiguration().set(TableInputFormat.SCAN,
        TableMapReduceUtil.convertScanToString(scan));
      if (addDependencyJars) {
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration(), TableMapReduceUtil.class);
      }
    }

    public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
            final Configuration conf,
            final HLog hlog)
                        throws IOException {
        Path tableDir =
        HTableDescriptor.getTableDir(rootDir, info.getTableDesc().getName());
        Path regionDir = HRegion.getRegionDir(tableDir, info.getEncodedName());
        FileSystem fs = FileSystem.get(conf);
        HLog effectiveHLog = hlog;
        if (hlog == null) {
        effectiveHLog = new HLog(fs, new Path(regionDir, HConstants.HREGION_LOGDIR_NAME),
        new Path(regionDir, HConstants.HREGION_OLDLOGDIR_NAME), conf);
        }
        HRegion region = HRegion.newHRegion(tableDir,
        effectiveHLog, fs, conf, info, null);
        region.initialize();
        return region;
   }
    
    /**
     * Get a list of the paths from underneath the path recursively.
     * @param hdfs
     * @param pPath
     * @return
     * @throws IOException
     */
    public static ArrayList<String> lsRecursive(FileSystem hdfs, Path pPath) throws IOException
    {
        ArrayList<String> ret = new ArrayList<String>();
        Stack<Path> stack = new Stack<Path>();
        
        stack.push(pPath);
        while(!stack.isEmpty())
        {
            Path path = stack.pop();
            for(FileStatus status: hdfs.listStatus(path))
            {
                if(status.isDir())
                {
                    stack.push(status.getPath());
                }
                else
                {
                    ret.add(status.getPath().toString());
                }
            }
        }
        
        return ret;

    
    }
   
    
    public static HRegion initHRegion (
            Configuration conf, HRegionInfo info)
          throws IOException{
            conf.setClass(HLOG_WRITER_IMPLEMENTATION_CLASS, DoNothingHLogWriter.class, HLog.Writer.class);
           //Configuration injection allows to set our own class
            conf.setClass(HConstants.REGION_IMPL, NoWritesHRegion.class, HRegion.class);

            HTableDescriptor descriptor = info.getTableDesc();
            //Make the Region read only.
            descriptor = new HTableDescriptor(descriptor);
            //Have to make a copy because we are served an Unmodifiable descriptor.
            descriptor.setReadOnly(true);

            info.setTableDesc(descriptor);
            Path p = new Path(HBASE_RELATIVE_DIR);
            
            FileSystem fs = FileSystem.get(conf);
            Path logDir = new Path("/tmp/" + HConstants.HREGION_LOGDIR_NAME);
            Path oldLogDir = new Path(HConstants.HREGION_OLDLOGDIR_NAME);
            HRegion region = HRegion.createHRegion(info, p, conf, new NoWritesHLog(fs, logDir, oldLogDir, conf));
            return region;
          }
    
    
    
    private static class NoWritesHLog extends HLog{

        public NoWritesHLog(FileSystem fs, Path dir, Path oldLogDir, Configuration conf)
                throws IOException {
            super(fs, dir, oldLogDir, conf, null, false, null);
        }
        
        @Override
        public void append(HRegionInfo regionInfo, WALEdit logEdit,
                final long now,
                final boolean isMetaRegion)
              throws IOException {
            throw new IOException("This is a read only log file.");
        }
        @Override
        public void sync(){
            //do nothing.
        }
        
        @Override
        public byte[][] rollWriter() throws FailedLogCloseException, IOException{
            try {
                Field field = HLog.class.getDeclaredField("hdfs_out");
                field.setAccessible(true);
                field.set(this, new FSDataOutputStream(new DoNothingFSDataOutputStream(new ByteArrayOutputStream()), new Statistics("hdfs://")));
            } catch (SecurityException e) {
                throw e;
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);

            } catch (IOException e) {
                throw new RuntimeException(e);

            }
            
            
            return null;
        }
        
        
        

    }
    

    private static class DoNothingFSDataOutputStream extends FilterOutputStream{

        public DoNothingFSDataOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        @Override
        public void write(byte[] b, int off, int len){}
        @Override
        public void write(int b){}
        @Override
        public void write(byte[] b){}
        
    }
    private static class DoNothingHLogWriter implements HLog.Writer{

        @Override
        public void init(FileSystem fs, Path path, Configuration c) throws IOException {            
            //Do nothing
        }

        @Override
        public void close() throws IOException {
            //Ignore
        }

        @Override
        public void sync() throws IOException {
            throw new RuntimeException(new OperationNotSupportedException("This is a dummy log to simulate a read only context."));            
        }

        @Override
        public void append(Entry entry) throws IOException {
            throw new RuntimeException(new OperationNotSupportedException("This is a dummy log to simulate a read only context."));            
            
        }

        @Override
        public long getLength() throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }
        
    }
}

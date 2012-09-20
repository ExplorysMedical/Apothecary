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
package org.apache.hadoop.hbase.regionserver;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionUtil;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.explorys.apothecary.hbase.mr.inputformat.HRegionInputSplit;
import com.explorys.apothecary.hbase.mr.inputformat.MergedStoreFileRecordReader;


public class HFileResultInputFormat extends InputFormat<ImmutableBytesWritable, Result> implements Configurable{
    public static final String TABLE_NAME_PARAM = "HFileInputFormatTableName";
    public static final int NUM_HOSTS=1;
    Scan scan = null;
    Configuration conf = null;
    public static final String SECONDS_TO_WAIT_PROP = "seconds.to.stall";
    public static final int MILLIS_TO_WAIT = 1000 * 60 * 5; //ten minutes to start
    public static final String IS_A_TASK_PROPERTY = "sleeping.only.on.client";
    public static final String IS_A_TASK_SENTINEL = "true";
    public static final String NUMBER_TABLE_FILES = "number.table.files";
    /*
     * 
     * We would like to wait for the time that flushing and minor compaction can take before running a job after 
     * requesting a flush and a compaction.
     * 
     * By querying the logs for the datanodes, we see that the maximum time for a flush is 90 seconds.
     * The times were viewed with this horrible command: (I have inserted newlines for readability, but don't know how they play with the quoting.
     * for i in $(ssh ops-server "cat /usr/local/explorys/scripts/jenkinsDeploy/syslist/datagrid.systems | grep datanode");do 
     *      ssh $i "cat /usr/lib/hbase/logs/\$(ls -tr /usr/lib/hbase/logs/ | grep region | egrep '.log$' | tail -n -1) | grep 'Finished memstore flush of' 
     *          | awk '{for (i=1;i <= NF;i++){print \$i}}' | egrep 'ms,\$' 
     *          | awk -F 'ms' '/[0-9]/{print \$1}' | sort -n | tail -n 1";
     * done
     * 
     * The idea is to query each datanode's most recent regionserver log for the lengthiest flush.
     * awk is used to split on space, then grep for '/alpha/ms,' then split on milliseconds and return the 
     * numbers in sorted order and take the greatest. The greatest amount of time for a flush was under 2 seconds excluding an
     * outlier of 90 seconds (probably during GC). So we will assume the worse and allow two minutes for flush.
     * 
     * 
     * It seems like even major compactions complete in a few minutes. And the queueing gives highest priority to user requests.
     * 
     */
    
    public void setScan(Scan scan){
        this.scan = scan;
    }
    
    /**
     * Get a record reader that will return Results from all the store files for a particular region.
     */
    @Override
    public RecordReader<ImmutableBytesWritable, Result> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        HRegionInputSplit hSplit = (HRegionInputSplit) split;
        HRegion region = hSplit.getRegion();
        return new MergedStoreFileRecordReader(region,scan);
    }

    /**
     * Called to determine the input splits for the job. Splits will request to be put on the same hostname as a regionserver.
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String tableName = conf.get(TABLE_NAME_PARAM);
        if (tableName == null){
            throw new IllegalArgumentException("Must provide a value for " + TABLE_NAME_PARAM);
        }

       
      HTable table = new HTable(tableName);  
      Map<HRegionInfo, HServerAddress> regionMap = table.getRegionsInfo();

        ArrayList<InputSplit> splits = new ArrayList<InputSplit>(regionMap.size());
        
        for (Entry<HRegionInfo, HServerAddress> entry : regionMap.entrySet()){
            HRegionInfo info = entry.getKey();
            HServerAddress address = entry.getValue();
            HRegionInputSplit split = new HRegionInputSplit(address.getHostname(), info);
            splits.add(split);
        }
        

        table.close();
        
        
        
        return splits;
    }
    
  
    /**
     * This method is called by the MapReduce Framework (JobRunner? I think) 
     * to configure the job. It is called both on the client side and by the individual tasks.
     * There is logic to allow that if it is on a client side, and the user hasn't specified a value 
     * for the constant IS_A_TASK_PROPERTY to IS_A_TASK_SENTINEL, then the job will request flush and 
     * compaction and wait an amount of time so that it will complete.
     */
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        if (conf.get(TableInputFormat.SCAN) != null) {
              try {
                ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(conf.get(TableInputFormat.SCAN)));
                DataInputStream dis = new DataInputStream(bis);
                scan = new Scan();
                scan.readFields(dis);

        		String isOnClient = conf.get(IS_A_TASK_PROPERTY);
        		if (isOnClient == null || !isOnClient.equals(IS_A_TASK_SENTINEL)){
                    String tableName = conf.get(TABLE_NAME_PARAM);
                    String secondsToWait = conf.get(SECONDS_TO_WAIT_PROP);
                    
                    Configuration hbaseConf = HBaseConfiguration.create(conf);
                    HBaseAdmin admin = new HBaseAdmin(hbaseConf);
                    admin.flush(tableName);
                    admin.compact(tableName);
                    
            		int millis = MILLIS_TO_WAIT;
        		    if (secondsToWait != null){
            		    millis = Integer.parseInt(secondsToWait) * 1000;
            		}
        		    
        		    System.out.println("Sleeping " + millis + " milliseconds to allow flush and compaction.");
        		    Threads.sleep(millis);
        		    
        		    conf.set(IS_A_TASK_PROPERTY, IS_A_TASK_SENTINEL);
        		    
        		}
                int numberOfTableFiles = HRegionUtil.lsRecursive(FileSystem.get(conf), new Path("/hbase/" + conf.get(TABLE_NAME_PARAM))).size();
        		conf.setInt(NUMBER_TABLE_FILES, numberOfTableFiles);
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              } catch (IllegalArgumentException e) {
                throw new RuntimeException(e);
              } catch (SecurityException e) {
                throw new RuntimeException(e);
              } catch (InterruptedException e){
    	         throw new RuntimeException(e);
    	      }
          }
    }
    @Override
    public Configuration getConf() {
        return conf;
    }
    
    

    

  

}

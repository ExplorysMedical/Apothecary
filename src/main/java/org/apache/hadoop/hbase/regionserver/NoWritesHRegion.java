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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.CancelableProgressable;


/**
 * This class will implement the method the same except for 
 * not obliterating the tmp data.
 * @author keith.wyss
 *
 */
public class NoWritesHRegion extends HRegion{
    
    public NoWritesHRegion(Path tableDir, HLog log, FileSystem fs, Configuration conf,
            HRegionInfo regionInfo, FlushRequester flushRequester){
        super(tableDir, log, fs, conf, regionInfo, flushRequester);
    }
    
    @Override
    public long initialize(final CancelableProgressable reporter) throws IOException{
        MonitoredTask status = TaskMonitor.get().createStatus(
                "Initializing region " + this);

            // A region can be reopened if failed a split; reset flags
            this.closing.set(false);
            this.closed.set(false);

            // Load in all the HStores.  Get maximum seqid.
            long maxSeqId = -1;
            for (HColumnDescriptor c : this.regionInfo.getTableDesc().getFamilies()) {
              status.setStatus("Instantiating store for column family " + c);
              Store store = instantiateHStore(this.tableDir, c);
              this.stores.put(c.getName(), store);
              long storeSeqId = store.getMaxSequenceId();
              if (storeSeqId > maxSeqId) {
                maxSeqId = storeSeqId;
              }
            }
            //This property is used to determine when to ignore recovered edits
            //They will be ignored if less than maxSeqId if replayRecovered edits is called.
            //Since this is fake Region, this is good.
            maxSeqId = Long.MAX_VALUE -1 ;

            
            //We don't want to do this in the subclass 
            
        //    status.setStatus("Cleaning up detritus from prior splits");
//            SplitTransaction.cleanupAnySplitDetritus(this);
//            FSUtils.deleteDirectory(this.fs, new Path(regiondir, MERGEDIR));
//

            //Hwe hwe hwe
            this.writestate.setReadOnly(true);

            this.writestate.compacting = false;
            // Use maximum of log sequenceid or that which was found in stores
            // (particularly if no recovered edits, seqid will be -1).
            long nextSeqid = maxSeqId + 1;
            LOG.info("Onlined " + this.toString() + "; next sequenceid=" + nextSeqid);

            status.markComplete("Region opened successfully");
            return nextSeqid;
    }
}

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
package com.explorys.apothecary.hbase.mr.inputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.CachedPeekHFilesScanner;
import org.apache.hadoop.hbase.regionserver.HFileResultInputFormat;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MergedStoreFileRecordReader extends RecordReader<ImmutableBytesWritable, Result> {

    HRegion region;
    Scan scan;
    int numberTableFiles;
    Path tableDir;
    boolean isDone;
    CachedPeekHFilesScanner scanner;
    FileSystem fs;
    
    public MergedStoreFileRecordReader(HRegion region, Scan scan) {
        this.region = region;
        this.scan = scan;
        isDone = false;
    }

    /**
     * In initialize we check the number of files in the table directory.
     * This is an invariant that will cause us to fail the job if it changes, because it is
     * likely that when this changes, the number of regions has changed as a result of a split.
     * 
     * We also initialize a scanner that handles merging KeyValues from storefiles 
     * (or Memstore although this is not currently accessible via the API)
     *  
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
  
        scanner = new CachedPeekHFilesScanner(region, scan);
        tableDir = region.getTableDir();
        fs = FileSystem.get(context.getConfiguration());
        int numFiles = HRegionUtil.lsRecursive(fs , tableDir).size();
        
        int numFilesConf = context.getConfiguration().getInt(HFileResultInputFormat.NUMBER_TABLE_FILES, 0);
        if (numFiles != numFilesConf){
            throw new IllegalStateException("Number of files has shifted underneath the job. Possible region split?");
        }
        numberTableFiles = numFiles;
        scanner.initialize();
    }

    /**
     * Retrieve the next KeyValue in order or finish or fail out if a region has split
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
       if (scanner.next() != null){
           return true;
       }
       else{
           isDone = true;
           int numTableFilesAfter = HRegionUtil.lsRecursive(fs, tableDir).size();
           if (numTableFilesAfter != numberTableFiles){
               throw new IllegalStateException("Number of files has shifted underneath the job. Possible region split?");
           }
           return false;
       }
    }

    /**
     * Exposes the row from the currentResult
     */
    @Override
    public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
        return new ImmutableBytesWritable(scanner.getCurrentResult().getRow());
    }

    /**
     * Called by the Mapper class, exposes the currentResult
     */
    @Override
    public Result getCurrentValue() throws IOException, InterruptedException {
        return scanner.getCurrentResult();
    }

    /**
     * Not sure is this is correct, but report 100% complete when finished with all the storeFiles.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
       if (!isDone){
        return 0;
       }
       return 1;
    }

    /**
     * Please do not call region.close(). It will possibly write out state, which is undesirable.
     * scanner.close() has almost no effect, although it sets a single value within each HFileScanner.
     * HBase has commented "Nothing to close in HFileScanner?" 
     * I guess not.
     */
    @Override
    public void close() throws IOException {
        scanner.close();
        /*
         * The region should explicitly not be closed because it flushes state.
         */
        // region.close();
    }

    


    
}

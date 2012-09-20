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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.mapreduce.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class HRegionInputSplit extends InputSplit implements Writable {

    String hostname;
    int countOfStoreFiles;
    HRegionInfo regionInfo;
    
    /**
     * Writable Constructor. Do not use.
     * @return
     */
    public HRegionInputSplit(){}
    
    public HRegion getRegion() throws IOException{
        return getRegion(HBaseConfiguration.create());
    }
    public HRegion getRegion(Configuration conf) throws IOException{
        return HRegionUtil.initHRegion(conf, regionInfo);
    }
    
    public HRegionInputSplit(String inputSplitHosts, HRegionInfo region) {
        
        hostname = inputSplitHosts;
        this.regionInfo = region;
    }

    //Need to provide something here. 
    //Can't spin up regions so can't get an approximation from storefiles.
    @Override
    public long getLength() throws IOException, InterruptedException {
        return hostname.length();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        
        return new String[]{hostname};
    }

    @Override
    public void write(DataOutput out) throws IOException {

                out.writeUTF(hostname);

            HRegionInfo info = regionInfo;
            info.write(out);
      
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        hostname = in.readUTF();
        
            regionInfo = new HRegionInfo();
            regionInfo.readFields(in);
       
    }
    
    

    


}

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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;

import com.explorys.apothecary.hbase.mr.inputformat.HRegionInputSplit;
import com.google.common.base.Function;

public class ScannerOpener {
    public static class HRegionSplitGenerator implements Function<HRegion, HRegionInputSplit>
    {
        
        Scan scan;
        public HRegionSplitGenerator(Scan scan){
            this.scan = scan;
        }

        @Override
        public HRegionInputSplit apply(HRegion region) {
            try {
                for (byte[] family : scan.getFamilies()){
                    Store store = region.getStore(family);
                    Method storeFileAccesor = store.getClass().getMethod("getStorefiles");
                    storeFileAccesor.setAccessible(true);
                    List<StoreFile> storeFiles =(List<StoreFile>) storeFileAccesor.invoke(store);
                    List<StoreFileScanner> sfScanners = StoreFileScanner
                            .getScannersForStoreFiles(storeFiles, true, false);
                    

                    
                }
               List<KeyValue> results = new ArrayList<KeyValue>();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (SecurityException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IllegalArgumentException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;
        }
    }
}

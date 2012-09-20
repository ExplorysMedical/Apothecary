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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class CachedPeekHFilesScanner implements ResultScanner {
    Scan scan;
    List<FamilyScanner> storeScanners;
    byte[] familyToDelete;
    byte[] columnToDelete;
    KeyValueAndBooleanPair controlPair;
    PriorityQueue<FamilyScanner> familyScanHeap = new PriorityQueue<FamilyScanner>();
    PriorityQueue<KeyValue> kvHeap = new PriorityQueue<KeyValue>(128, KeyValue.COMPARATOR);
    Result currentResult = null;
    boolean hasDeleteFamilies;
    public CachedPeekHFilesScanner(HRegion region, Scan scan){
        this.storeScanners = new ArrayList<FamilyScanner>();
        try {
            for (byte[] family : scan.getFamilies()){
                Store store = region.getStore(family);
                Method storeFileAccesor = store.getClass().getDeclaredMethod("getStorefiles");
                storeFileAccesor.setAccessible(true);
                //Have to do reflection to get this Method.
                @SuppressWarnings("unchecked")
                List<StoreFile> storeFiles =(List<StoreFile>) storeFileAccesor.invoke(store);
                List<StoreFileScanner> sfScanners = StoreFileScanner
                        .getScannersForStoreFiles(storeFiles, true, false);              
                for (StoreFileScanner sfScanner : sfScanners){
                    storeScanners.add(new FamilyScanner(sfScanner, family));
                }
            }
            this.scan = scan;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) { 
            throw new RuntimeException(e);
        }
    }
    
    
    public void initialize() throws IOException{
        
        for (int i = 0; i < storeScanners.size(); ++i){
            FamilyScanner scanner = storeScanners.get(i);
            StoreFileScanner sfScanner = scanner.getScanner();
            byte[] startRow;
            controlPair = new KeyValueAndBooleanPair();
            startRow = scan.getStartRow();
            if (startRow == null){
                startRow = HConstants.EMPTY_START_ROW;
            }
            boolean hasValues = sfScanner.seek(new KeyValue(startRow, scanner.getFamily(), new byte[]{}));
            if (!hasValues){
                scanner.setOutOfValues();

            }
            else{
                familyScanHeap.add(scanner);
            }
        }
        hasDeleteFamilies = false;
    }
    
   

    /**
     * Do not call this method. The next functionality requires initialization, so
     * you would be in a world of hurt.
     */
    @Override
    public Iterator<Result> iterator() {
        //Iterator requires initialization. Explicitly call next.
        throw new RuntimeException("Don't call this. You have to initialize before you call next.");
    }

    /**
     * You must call initialize before all of this.
     * Gets the next result.
     */
    @Override
    public Result next() throws IOException {
        try{
            KeyValue[] raw = nextRow();
            Result result = new Result(raw);
            currentResult = result;
            return result;
        }
        catch(NoSuchElementException e){
            return null;
        }

    }
    public Result getCurrentResult(){
        return currentResult;
    }

    public KeyValue[] nextRow() throws IOException, NoSuchElementException{
            
            KeyValue currentKeyValue = currentKV();
            
            byte[] currentRow = currentKeyValue.getRow();
            if (currentKeyValue.isDelete()){
               do{
                    if (currentKeyValue.isDeleteFamily()){
                        try{
                            currentKeyValue  = processKeyValuesMaskingFamily(currentKeyValue.getFamily(), currentRow, currentKeyValue, kvHeap);
                        }
                        catch(NoSuchElementException e){
                            if (kvHeap.size() > 0){
                                return buildKeyValArrayFromHeap(kvHeap);
                            }
                            else{
                                throw e;
                            }
                        }
                        if (kvHeap.size() > 0){
                            if (!Bytes.equals(currentKeyValue.getRow(), currentRow)){
                                return buildKeyValArrayFromHeap(kvHeap);
                            }
                        }
                    }
                    else{ //if kv.isDeleteColumn
                        try{
                            currentKeyValue = processKeyValuesMaskingColumn(currentKeyValue.getFamily(), currentKeyValue.getQualifier(), currentRow, currentKeyValue, kvHeap);
                        }
                        catch (NoSuchElementException e){
                            if (kvHeap.size() > 0){
                                return buildKeyValArrayFromHeap(kvHeap);
                            }
                            else{
                                throw e;
                            }
                        }
                        if (kvHeap.size() > 0){
                            if (!Bytes.equals(currentKeyValue.getRow(), currentRow)){
                                return buildKeyValArrayFromHeap(kvHeap);
                            }
                            else{
                                currentRow = currentKeyValue.getRow();
                            }
                        }
                    }
                }  while (currentKeyValue.isDeleteColumnOrFamily());
            }
            else{
                kvHeap.add(nextKV()); //This one is good. Get the next KV
            }
            
            boolean onCurrentRow = true;
            
            while (onCurrentRow){
                FamilyScanner famScan = familyScanHeap.peek();
                if (famScan == null){
                    onCurrentRow = false;
                }
                else{
                    KeyValue kv = famScan.peek();
                    if (Bytes.equals(kv.getRow(), currentRow)){
                        if (!kv.isDelete()){
                            kvHeap.add(nextKV());
                        }
                        else{
                            do{
                                if (kv.isDeleteFamily()){
                                    try{
                                        kv = processKeyValuesMaskingFamily(kv.getFamily(), currentRow, kv, kvHeap);
                                        //Adds all qualifying values to the heap and returns the next one after the scope of the delete runs out.
                                    }
                                    catch (NoSuchElementException e){
                                        return buildKeyValArrayFromHeap(kvHeap);
                                    }
                                    if (!Bytes.equals(kv.getRow(), currentRow)){
                                        onCurrentRow = false;
                                        break;
                                    }
                                    if (!kv.isDelete()){
                                        try{
                                            kvHeap.add(nextKV());
                                        }
                                        catch(NoSuchElementException e){
                                            return buildKeyValArrayFromHeap(kvHeap);
                                        }
                                        break;
                                    }
                                }
                                else{ // if kv.isDeleteColumn
                                    try{
                                        kv = processKeyValuesMaskingColumn(kv.getFamily(), kv.getQualifier(), currentRow, kv, kvHeap);
                                        //Adds all qualifying values to the heap and returns the next one after the scope of the delete runs out.
                                    }
                                    catch(NoSuchElementException e){
                                        return buildKeyValArrayFromHeap(kvHeap);
                                    }
                                    if (!Bytes.equals(kv.getRow(), currentRow)){
                                        onCurrentRow = false;
                                        break;
                                    }
                                    if (!kv.isDelete()){
                                        try{
                                            kvHeap.add(nextKV());
                                        }
                                        catch(NoSuchElementException e){
                                            return buildKeyValArrayFromHeap(kvHeap);
                                        }
                                        break;
                                    }
                                }
                            }while (true);
                        }
                    }
                    else{
                        onCurrentRow = false;
                    }
               }
            }
            return buildKeyValArrayFromHeap(kvHeap);
    }
    
    //Called with the top of the heap as the delete column KV
    private KeyValue processKeyValuesMaskingColumn(byte[] family, byte[] qualifier, byte[] currentRow, KeyValue deleteColumn, PriorityQueue<KeyValue> heap) throws NoSuchElementException, IOException {
        KeyValue underConsideration;
        while(true){
            nextKV();
            underConsideration = currentKV();
            if (!Bytes.equals(family, underConsideration.getFamily())){
                break;
            }
            if (!Bytes.equals(qualifier, underConsideration.getQualifier())){
                break;
            }
            if (!Bytes.equals(currentRow, underConsideration.getRow())){
                break;
            }
            if (KeyValue.COMPARATOR.compareTimestamps(underConsideration, deleteColumn) < 0){
                heap.add(underConsideration);
            }

        }
        // We want these byte accesses in the condition so that they can short circuit
        return currentKV();
    }


    private KeyValue[] buildKeyValArrayFromHeap(PriorityQueue<KeyValue> heap){
        KeyValue[] kvs = new KeyValue[heap.size()];
        KeyValue keyVal;
        for (int i = 0; i < kvs.length; i++){
            keyVal = heap.poll();
            kvs[i] = keyVal;
        } 
        return kvs;
    }
    
    //Called with the scanner pointing to the current DeleteFamily KeyValue
    private KeyValue processKeyValuesMaskingFamily(byte[] family, byte[] currentRow, KeyValue deleteFamily, PriorityQueue<KeyValue> heap) throws NoSuchElementException, IOException {
        KeyValueAndBooleanPair contPair = controlPair;
        nextKV();
        KeyValue underConsideration = currentKV();
        contPair.kv = underConsideration;
        contPair.bool = true;
        while (contPair.bool){
            evaluateIncludeKeyValueWithMaskingFamily(family, currentRow, deleteFamily, heap, contPair);
        }
        contPair.bool = true;
        return contPair.kv;
    }

    //Called with the scanner pointing to the record that is being evaluated
    private void evaluateIncludeKeyValueWithMaskingFamily(byte[] family, byte[] currentRow, KeyValue deleteFamily, PriorityQueue<KeyValue> heap, KeyValueAndBooleanPair contPair)throws NoSuchElementException, IOException{
        KeyValue underConsideration = contPair.kv;
        if (!Bytes.equals(family, underConsideration.getFamily())){
            controlPair.bool = false;
            return;
        }
        if (!Bytes.equals(currentRow, underConsideration.getRow())){
            controlPair.bool = false;
            return;
        }
        if (KeyValue.COMPARATOR.compareTimestamps(underConsideration, deleteFamily) < 0){
            byte type = underConsideration.getType();
            if (type == KeyValue.Type.Put.getCode()){
                heap.add(underConsideration);
                 nextKV();
                 contPair.kv = currentKV();
                controlPair.bool = true;
                return;
            }
            else{
                
                 if ( underConsideration.isDelete()){ //Can't be a different family delete because we already checked the families matched.
                     // and this Timestamp is more recent, so that would have masked out.
                    //TODO check this below
                    if (KeyValue.COMPARATOR.compareTimestamps(deleteFamily, underConsideration) > 0){ //deleteFamily is less recent.
                        KeyValue kv = processKeyValuesMaskingColumn(family, underConsideration.getQualifier(), currentRow, underConsideration, heap);
                        contPair.kv = kv;
                        evaluateIncludeKeyValueWithMaskingFamily(family, currentRow, deleteFamily, heap, contPair);
                        return;
                    }
                }

            }
        }
        nextKV();
        contPair.kv = currentKV();
        return;
    }
    
    public static class KeyValueAndBooleanPair{
        public KeyValue kv;
        public boolean bool;
    }
    
    protected KeyValue nextKV() throws NoSuchElementException, IOException{
       FamilyScanner scanner = familyScanHeap.remove();
       
       KeyValue currentkeyValue = scanner.next();
       if (scanner.peek() != null){
           familyScanHeap.add(scanner);
       }
       return currentkeyValue;
    }
    protected KeyValue currentKV() throws IOException{
        FamilyScanner famScan = familyScanHeap.peek();
        if (famScan != null){
            return famScan.peek();
        }
        else{
            throw new NoSuchElementException();
        }
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
        return null;
    }


    @Override
    public void close() {
        for (FamilyScanner store : storeScanners){
            store.getScanner().close();
        }
    }
    
    public static class FamilyScanner implements Comparable<FamilyScanner>{
        byte[] family;
        StoreFileScanner sfScanner;
        boolean hasValues = true;
        Comparator<KeyValue> comparator = KeyValue.COMPARATOR;

        public FamilyScanner(StoreFileScanner scanner, byte[] family){
            this.sfScanner = scanner;
            this.family = family;
        }
        public StoreFileScanner getScanner(){
            return sfScanner;
        }
        public byte[] getFamily(){
            return family;   
        }
        public void setOutOfValues(){
            hasValues = false;
        }
        public boolean hasValues(){
            if (hasValues){
                return true;
            }
            return false;
        }
        public KeyValue peek(){
            return sfScanner.peek();
        }
        public KeyValue next() throws IOException{
            return sfScanner.next();
        }
        
        @Override
        public int compareTo(FamilyScanner o) {
            return comparator.compare(peek(), o.peek());
        }
    }
    
}

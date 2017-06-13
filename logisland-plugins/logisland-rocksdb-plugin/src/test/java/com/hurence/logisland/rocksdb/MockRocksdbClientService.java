/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.rocksdb;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.service.hbase.HBaseClientService;
import com.hurence.logisland.service.hbase.put.PutColumn;
import com.hurence.logisland.service.hbase.put.PutRecord;
import com.hurence.logisland.service.hbase.scan.Column;
import com.hurence.logisland.service.hbase.scan.ResultCell;
import com.hurence.logisland.service.hbase.scan.ResultHandler;
import com.hurence.logisland.service.rocksdb.RocksdbClientService;
import com.hurence.logisland.service.rocksdb.delete.DeleteRequest;
import com.hurence.logisland.service.rocksdb.delete.DeleteResponse;
import com.hurence.logisland.service.rocksdb.get.GetRequest;
import com.hurence.logisland.service.rocksdb.get.GetResponse;
import com.hurence.logisland.service.rocksdb.put.ValuePutRequest;
import com.hurence.logisland.service.rocksdb.scan.RocksIteratorHandler;
import org.rocksdb.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MockRocksdbClientService extends AbstractControllerService implements RocksdbClientService {

    private final Map<String,Map<String,byte[]>> db = new HashMap<>();
    private final static String defaultFamily = "default";

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return null;
    }

    @Override
    public void multiPut(Collection<ValuePutRequest> puts) throws RocksDBException {
        for(ValuePutRequest req: puts) {
            put(req);
        }
    }

    @Override
    public void put(ValuePutRequest put) throws RocksDBException {
        put(put.getFamily(), put.getKey(), put.getValue());
    }

    @Override
    public void put(String familyName, byte[] key, byte[] value) throws RocksDBException {
        Map<String,byte[]> familyDb = db.get(familyName);
        if (familyDb==null) {
            familyDb = new HashMap<>();
            db.put(familyName, familyDb);
        }
        familyDb.put(new String(key, StandardCharsets.UTF_8), value);
    }

    @Override
    public void put(byte[] key, byte[] value) throws RocksDBException {
        put(defaultFamily, key, value);
    }

    @Override
    public void put(String familyName, byte[] key, byte[] value, WriteOptions writeOptions) throws RocksDBException {
        put(familyName, key, value);
    }

    @Override
    public void put(byte[] key, byte[] value, WriteOptions writeOptions) throws RocksDBException {
        put(defaultFamily, key, value);
    }

    @Override
    public Collection<GetResponse> multiGet(Collection<GetRequest> getRequests) throws RocksDBException {
        Collection<GetResponse> toReturn = new ArrayList<>();
        for(GetRequest req: getRequests) {
            toReturn.add(get(req));
        }
        return toReturn;
    }

    @Override
    public GetResponse get(GetRequest getRequest) throws RocksDBException {
        byte[] value = get(getRequest.getFamily(), getRequest.getKey());
        GetResponse resp = new GetResponse();
        resp.setValue(value);
        resp.setKey(getRequest.getKey());
        resp.setFamily(getRequest.getFamily());
        return resp;
    }

    @Override
    public byte[] get(byte[] key) throws RocksDBException {
        return get(defaultFamily, key);
    }

    @Override
    public byte[] get(byte[] key, ReadOptions rOption) throws RocksDBException {
        return get(defaultFamily, key);
    }

    @Override
    public byte[] get(String familyName, byte[] key) throws RocksDBException {
        Map<String, byte[]> fDb = db.get(familyName);
        if (fDb ==null) {
            return null;
        }else {
            return fDb.get(new String(key, StandardCharsets.UTF_8));
        }
    }

    @Override
    public byte[] get(String familyName, byte[] key, ReadOptions rOption) throws RocksDBException {
        return get(familyName, key);
    }

    @Override
    public Collection<DeleteResponse> multiDelete(Collection<DeleteRequest> deleteRequests) throws RocksDBException {
        Collection<DeleteResponse> toReturn = new ArrayList<>();
        for(DeleteRequest req: deleteRequests) {
            toReturn.add(delete(req));
        }
        return toReturn;
    }

    @Override
    public DeleteResponse delete(DeleteRequest deleteRequest) throws RocksDBException {
        DeleteResponse dresp = new DeleteResponse();
        dresp.setFamily(deleteRequest.getFamily());
        dresp.setKey(deleteRequest.getKey());
        delete(deleteRequest.getFamily(), deleteRequest.getKey());
        return dresp;
    }

    @Override
    public void delete(byte[] key) throws RocksDBException {
        delete(defaultFamily, key);
    }

    @Override
    public void delete(byte[] key, WriteOptions wOption) throws RocksDBException {
        delete(defaultFamily, key);
    }

    @Override
    public void delete(String familyName, byte[] key) throws RocksDBException {
        Map<String, byte[]> fDb = db.get(familyName);
        if (fDb !=null) {
            fDb.remove(new String(key, StandardCharsets.UTF_8));
        }
    }

    @Override
    public void delete(String familyName, byte[] key, WriteOptions wOption) throws RocksDBException {
        delete(familyName, key);
    }

    @Override
    public void deleteRange(byte[] keyStart, byte[] keyEnd) throws RocksDBException {

    }

    @Override
    public void deleteRange(byte[] keyStart, byte[] keyEnd, WriteOptions wOption) throws RocksDBException {

    }

    @Override
    public void deleteRange(String familyName, byte[] keyStart, byte[] keyEnd) throws RocksDBException {

    }

    @Override
    public void deleteRange(String familyName, byte[] keyStart, byte[] keyEnd, WriteOptions wOption) throws RocksDBException {

    }

    @Override
    public void scan(RocksIteratorHandler handler) throws RocksDBException {

    }

    @Override
    public void scan(String familyName, RocksIteratorHandler handler) throws RocksDBException {

    }

    @Override
    public void scan(String familyName, ReadOptions rOptions, RocksIteratorHandler handler) throws RocksDBException {

    }

    @Override
    public RocksDB getDb() {
        return null;
    }

    @Override
    public Map<String, ColumnFamilyHandle> getFamilies() {
        return null;
    }
}

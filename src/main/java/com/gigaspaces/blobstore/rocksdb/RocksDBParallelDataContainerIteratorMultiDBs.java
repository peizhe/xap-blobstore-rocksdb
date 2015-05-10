package com.gigaspaces.blobstore.rocksdb;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.server.blobstore.BlobStoreGetBulkOperationResult;
import com.gigaspaces.server.blobstore.BlobStoreObjectType;
import com.j_spaces.core.cache.offHeap.storage.BlobStoreSegmentedParallelIterator;


/**
 * see com.j_spaces.core.cache.offHeap.storage.BlobStoreSegmentedParallelIterator
 * @author Kobi
 * @since 10.0.0
 */
public class RocksDBParallelDataContainerIteratorMultiDBs extends BlobStoreSegmentedParallelIterator {

    private final RocksDBBlobStoreHandlerMultiDBs handler;
    private final BlobStoreObjectType objectType;

    public RocksDBParallelDataContainerIteratorMultiDBs(RocksDBBlobStoreHandlerMultiDBs handler, int dbsNumber, BlobStoreObjectType objectType) {
        super(dbsNumber);
        this.handler = handler;
        this.objectType = objectType;
    }

    public RocksDBBlobStoreHandlerMultiDBs getHandler() {
        return handler;
    }

    @Override
    public DataIterator<BlobStoreGetBulkOperationResult> createDataIteratorForSegmen(int dbsNumber) {

        return  new RocksDBContainerIteratorMultiDBs(handler, dbsNumber, objectType);
    }
}

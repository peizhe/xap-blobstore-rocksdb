package com.gigaspaces.blobstore.rocksdb;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.server.blobstore.BlobStoreGetBulkOperationResult;
import com.gigaspaces.server.blobstore.BlobStoreObjectType;
import com.j_spaces.core.cache.offHeap.storage.BlobStoreSegmentedParallelIterator;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.List;


/**
 * see com.j_spaces.core.cache.offHeap.storage.BlobStoreSegmentedParallelIterator
 * @author Kobi
 * @since 10.0.0
 */
public class RocksDBParallelDataContainerIterator extends BlobStoreSegmentedParallelIterator {

    private final RocksDB db;
    private final BlobStoreObjectType objectType;
    private final List<ColumnFamilyHandle> columnFamilyHandles;

    public RocksDBParallelDataContainerIterator(RocksDB db, int numberOfColumns, BlobStoreObjectType objectType, List<ColumnFamilyHandle> columnFamilyHandles) {
        super(numberOfColumns);
        this.db = db;
        this.columnFamilyHandles = columnFamilyHandles;
        this.objectType = objectType;
    }

    public RocksDB getDb() {
        return db;
    }

    @Override
    public DataIterator<BlobStoreGetBulkOperationResult> createDataIteratorForSegmen(int segmentNumber) {

        return  new RocksDBContainerIterator(db, segmentNumber, objectType, columnFamilyHandles);
    }
}

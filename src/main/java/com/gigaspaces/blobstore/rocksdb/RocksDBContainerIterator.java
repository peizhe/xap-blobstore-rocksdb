package com.gigaspaces.blobstore.rocksdb;

import com.gigaspaces.blobstore.rocksdb.config.Constants;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.server.blobstore.BlobStoreException;
import com.gigaspaces.server.blobstore.BlobStoreGetBulkOperationResult;
import com.gigaspaces.server.blobstore.BlobStoreObjectType;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * Creates an iterator that can be used to exhaustively for retrieving data from FDF Container.
 * Uses com.sandisk.fdf.FDFEnumerator.
 *
 * @author Kobi
 * @version 10.0.0
 */
public class RocksDBContainerIterator implements DataIterator<BlobStoreGetBulkOperationResult> {

    private static final Logger logger = Logger.getLogger(Constants.STORAGE_HANDLER);
    private final int columnIndex;
    private RocksIterator rocksDBIterator;
    private boolean isFinish;
    private boolean initialized = false;
    private boolean closed = false;
    private final BlobStoreObjectType objectType;
    private final RocksDB db;
    private final List<ColumnFamilyHandle> columnFamilyHandles;

    public RocksDBContainerIterator(RocksDB db, int columnIndex, BlobStoreObjectType objectType, List<ColumnFamilyHandle> columnFamilyHandles) {
        this.db = db;
        this.columnIndex = columnIndex;
        this.objectType = objectType;
        this.columnFamilyHandles = columnFamilyHandles;
    }

    public RocksDBContainerIterator(RocksDB db, BlobStoreObjectType objectType, List<ColumnFamilyHandle> columnFamilyHandles) {
        this.db = db;
        this.objectType = objectType;
        this.columnIndex = 0;
        this.columnFamilyHandles = columnFamilyHandles;
    }

    @Override
    public void close() {
        try {
            if(closed)
                logger.warning("Trying to close FDFContainerIterator but it already closed. Iterator properties " +
                        "[ object type = " +objectType + ", container index = " + columnIndex + " ].");
            if(initialized && !closed){
                isFinish = true;
                closed=true;
            }
        } catch (Exception e) {
            throw new BlobStoreException(e);
        }
    }

    @Override
    public boolean hasNext() {
        if(!initialized){
            initialize();
            initialized = true;
        }
        if(isFinish)
            return false;
        isFinish = !rocksDBIterator.isValid();
        return !isFinish;
    }

    @Override
    public BlobStoreGetBulkOperationResult next() {
        if(!initialized){
            initialize();
            initialized = true;
        }
        rocksDBIterator.next();

        return new BlobStoreGetBulkOperationResult(rocksDBIterator.key(), rocksDBIterator.value(), null);
    }

    private void initialize(){
        if(objectType.equals(BlobStoreObjectType.METADATA)){
            rocksDBIterator = db.newIterator(columnFamilyHandles.get(Constants.ROCKSDB_COLUMNS_DEFAULT));
            rocksDBIterator.seekToLast();
        }else{
            rocksDBIterator = db.newIterator(columnFamilyHandles.get(columnIndex));
            rocksDBIterator.seekToLast();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove is not supported");
    }
}

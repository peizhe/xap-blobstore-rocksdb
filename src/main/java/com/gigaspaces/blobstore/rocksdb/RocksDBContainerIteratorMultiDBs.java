package com.gigaspaces.blobstore.rocksdb;

import com.gigaspaces.blobstore.rocksdb.config.Constants;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.server.blobstore.BlobStoreException;
import com.gigaspaces.server.blobstore.BlobStoreGetBulkOperationResult;
import com.gigaspaces.server.blobstore.BlobStoreObjectType;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.logging.Logger;

/**
 * Creates an iterator that can be used to exhaustively for retrieving data from FDF Container.
 * Uses com.sandisk.fdf.FDFEnumerator.
 *
 * @author Kobi
 * @version 10.0.0
 */
public class RocksDBContainerIteratorMultiDBs implements DataIterator<BlobStoreGetBulkOperationResult> {

    private static final Logger logger = Logger.getLogger(Constants.STORAGE_HANDLER);
    private final int dbIndex;
    private RocksIterator rocksDBIterator;
    private boolean isFinish;
    private boolean initialized = false;
    private boolean closed = false;
    private final BlobStoreObjectType objectType;
    private RocksDBBlobStoreHandlerMultiDBs handler;

    public RocksDBContainerIteratorMultiDBs(RocksDBBlobStoreHandlerMultiDBs handler, int dbIndex, BlobStoreObjectType objectType) {
        this.handler = handler;
        this.dbIndex = dbIndex;
        this.objectType = objectType;
    }

    public RocksDBContainerIteratorMultiDBs(RocksDBBlobStoreHandlerMultiDBs handler, BlobStoreObjectType objectType) {
        this.handler = handler;
        this.objectType = objectType;
        this.dbIndex = 0;
    }

    @Override
    public void close() {
        try {
            if(closed)
                logger.warning("Trying to close FDFContainerIterator but it already closed. Iterator properties " +
                        "[ object type = " +objectType + ", db index = " + dbIndex + " ].");
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

        BlobStoreGetBulkOperationResult result = new BlobStoreGetBulkOperationResult(rocksDBIterator.key(), rocksDBIterator.value(), null);
        rocksDBIterator.next();
        return result;
    }

    private void initialize(){
        if(objectType.equals(BlobStoreObjectType.METADATA)){
            rocksDBIterator = handler.createMetadataRocksIterator();
            rocksDBIterator.seekToFirst();
        }else{
            rocksDBIterator = handler.createDataRocksIterator(dbIndex);
            rocksDBIterator.seekToFirst();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove is not supported");
    }
}

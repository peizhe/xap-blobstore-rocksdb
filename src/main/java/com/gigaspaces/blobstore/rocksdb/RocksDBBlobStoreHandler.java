package com.gigaspaces.blobstore.rocksdb;

import com.gigaspaces.blobstore.rocksdb.config.Constants;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.server.blobstore.*;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * @author kobi on 5/5/15.
 * @since 10.2
 */
public class RocksDBBlobStoreHandler extends BlobStoreStorageHandler {

    private static final Logger logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);
    private final int numberOfColumns = Integer.parseInt(System.getProperty(Constants.ROCKSDB_COLUMNS, String.valueOf(Constants.ROCKSDB_COLUMNS_DEFAULT)));
    private final int blobStoreBulkMinThreads = Integer.getInteger(Constants.BLOBSTORE_BULK_POOL_MIN_THREADS, Constants.BLOBSTORE_BULK_POOL_MIN_THREADS_DEFAULT);
    private final int blobStoreBulkMaxThreads = Integer.getInteger(Constants.BLOBSTORE_BULK_POOL_MAX_THREADS, Constants.BLOBSTORE_BULK_POOL_MAX_THREADS_DEFAULT);

    private static final java.lang.String DB_PATH = "/tmp/rocksdb";
    private RocksDB db = null;
    private DBOptions options = null;
    private List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<ColumnFamilyHandle>();
    private ExecutorService bulkThreadPool = null;


    @Override
    public void initialize(String spaceName, Properties properties, boolean warm) {

        String dbPath = DB_PATH;

        options = new DBOptions();
        options.setCreateIfMissing(true);

        db = createDB(spaceName, numberOfColumns, dbPath, warm);

        bulkThreadPool = new ThreadPoolExecutor(blobStoreBulkMinThreads, blobStoreBulkMaxThreads,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());


        logger.info("RocksDB Storage Handler [ " + getClass().getName() + " ] initialized on space: " + spaceName);
    }

    @Override
    public Properties getProperties(){
        Properties configProperties = new Properties();
        return configProperties;
    }

    @Override
    public Object add(Serializable id, Serializable data, BlobStoreObjectType objectType) {
        return objectType == BlobStoreObjectType.DATA ? add(id, data, columnFamilyHandles.get(getContainerNumberFromId(id))) :
                add(id, data, columnFamilyHandles.get(numberOfColumns));
    }

    private Object add(Serializable id, Serializable data, ColumnFamilyHandle columnFamilyHandle) {
        try{
            db.put(columnFamilyHandle, id.toString().getBytes(), (byte[])data);
        }catch (RocksDBException e){
            throw new BlobStoreException(e);
        }
        return -1;
    }


    @Override
    public Serializable get(Serializable id, Object position, BlobStoreObjectType objectType) {
        return objectType == BlobStoreObjectType.DATA ? get(id, position, columnFamilyHandles.get(getContainerNumberFromId(id))) :
                get(id, position, columnFamilyHandles.get(numberOfColumns));
    }

    private Serializable get(java.io.Serializable id, Object position, ColumnFamilyHandle columnFamilyHandle) {
        Serializable data;
        try {
            data = db.get(columnFamilyHandle, id.toString().getBytes());
        }catch (RocksDBException e){
            throw new BlobStoreException(e);
        }
        if (data == null && columnFamilyHandle == columnFamilyHandles.get(getContainerNumberFromId(id)))
            return null;
        else
            return data;
    }



    @Override
    public Object replace(Serializable id, Serializable data, Object position, BlobStoreObjectType objectType) {
        return objectType == BlobStoreObjectType.DATA ? replace(id, data, position, columnFamilyHandles.get(getContainerNumberFromId(id))) :
                replace(id, data, position, columnFamilyHandles.get(numberOfColumns));
    }


    private Object replace(java.io.Serializable id,java.io.Serializable data,  Object position, ColumnFamilyHandle columnFamilyHandle) {
        try {
            db.merge(columnFamilyHandle, id.toString().getBytes(), (byte[]) data);
        }catch (RocksDBException e){
            throw new BlobStoreException(e);
        }
        return -1;
    }


    @Override
    public Serializable remove(Serializable id, Object position, BlobStoreObjectType objectType) {
        if (objectType == BlobStoreObjectType.DATA)
            return remove(id, position, columnFamilyHandles.get(getContainerNumberFromId(id)));
        else
            return remove(id, position, columnFamilyHandles.get(numberOfColumns));
    }

    private Serializable remove(java.io.Serializable id, Object position,ColumnFamilyHandle columnFamilyHandle) {
        try {
            db.remove(id.toString().getBytes());
        }catch (RocksDBException e){
            throw new BlobStoreException(e);
        }
        return null;
    }

    @Override
    public List<BlobStoreBulkOperationResult> executeBulk(List<BlobStoreBulkOperationRequest> operations, BlobStoreObjectType objectType, boolean transactional) {
        List<BlobStoreBulkOperationResult> blobStoreBulkOperationResults = null;

        blobStoreBulkOperationResults = super.executeBulk(operations, objectType, transactional);

        return blobStoreBulkOperationResults;
    }

    @Override
    public DataIterator<BlobStoreGetBulkOperationResult> iterator(BlobStoreObjectType objectType) {
        if(objectType.equals(BlobStoreObjectType.METADATA)){
            return new RocksDBContainerIterator(db, objectType, columnFamilyHandles);
        }else{
            return getIterator(objectType);
        }
    }

    @Override
    public void close() {
        for (ColumnFamilyHandle handle : columnFamilyHandles){
            handle.dispose();
        }
        if (db != null) {
            db.close();
        }
    }

    private synchronized RocksDB createDB(String spaceName, int columnFamilyNumber, String dbPath, boolean warm){
        String columnFamilyName;
        try{
            if(warm){
                List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<ColumnFamilyDescriptor>();
                for(int i = 0; i < columnFamilyNumber; i++) {
                    columnFamilyName = spaceName + "_gs_" + i;
                    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                            columnFamilyName.getBytes(), new ColumnFamilyOptions()));
                }
                columnFamilyName = spaceName + "_metadata";
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                        columnFamilyName.getBytes(), new ColumnFamilyOptions()));

                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                        RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));

                return RocksDB.open(new DBOptions(), dbPath,
                        columnFamilyDescriptors, columnFamilyHandles);
            }else {
                db = RocksDB.open(new Options().setCreateIfMissing(true), dbPath);
                for (int i = 0; i < columnFamilyNumber; i++) {
                    columnFamilyName = spaceName + "_gs_" + i;
                    columnFamilyHandles.add(db.createColumnFamily(new ColumnFamilyDescriptor(columnFamilyName.getBytes(),
                            new ColumnFamilyOptions())));
                }
                columnFamilyName = spaceName + "_metadata";
                columnFamilyHandles.add(db.createColumnFamily(new ColumnFamilyDescriptor(columnFamilyName.getBytes(),
                        new ColumnFamilyOptions())));
                return db;
            }

        }catch (RocksDBException e){
            throw new BlobStoreException(e);
        }
    }

    private int getContainerNumberFromId(Serializable id){
        return (Math.abs(id.hashCode())) % numberOfColumns;
    }

    public RocksDBParallelDataContainerIterator getIterator(BlobStoreObjectType objectType) {
        return new RocksDBParallelDataContainerIterator(db, numberOfColumns, objectType, columnFamilyHandles);
    }
}

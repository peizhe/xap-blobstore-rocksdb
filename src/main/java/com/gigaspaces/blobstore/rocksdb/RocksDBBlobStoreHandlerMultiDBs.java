package com.gigaspaces.blobstore.rocksdb;

import com.gigaspaces.blobstore.rocksdb.config.Constants;
import com.gigaspaces.blobstore.rocksdb.utils.Utils;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.server.blobstore.*;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * @author kobi on 5/5/15.
 * @since 10.2
 */
public class RocksDBBlobStoreHandlerMultiDBs extends BlobStoreStorageHandler {

    private static final Logger logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);
    private final int dbsNumber = Integer.parseInt(System.getProperty(Constants.ROCKSDB_COLUMNS, String.valueOf(Constants.ROCKSDB_COLUMNS_DEFAULT)));
    private final int blobStoreBulkMinThreads = Integer.getInteger(Constants.BLOBSTORE_BULK_POOL_MIN_THREADS, Constants.BLOBSTORE_BULK_POOL_MIN_THREADS_DEFAULT);
    private final int blobStoreBulkMaxThreads = Integer.getInteger(Constants.BLOBSTORE_BULK_POOL_MAX_THREADS, Constants.BLOBSTORE_BULK_POOL_MAX_THREADS_DEFAULT);

    private static final BlobStoreBulkOperationResult nullIndicator = new BlobStoreAddBulkOperationResult(null, null);

    private String dbPath;
    Options options;
    private List<RocksDB> dataDBs;
    private RocksDB metadataDB;
    private ExecutorService bulkThreadPool = null;
//    private final static RocksEnv rocksEnv;

    //    static {
//        rocksEnv = (RocksEnv) RocksEnv.getDefault();
//        rocksEnv.setBackgroundThreads(5, RocksEnv.FLUSH_POOL);
//        rocksEnv.setBackgroundThreads(5, RocksEnv.COMPACTION_POOL);
//    }
    @Override
    public void initialize(String spaceName, Properties properties, boolean warm) {
        options = new Options();
        options.setMemTableConfig(new SkipListMemTableConfig());
        BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
        tableOptions.setBlockSize(1024)
                .setBlockCacheSize(100000000)
                .setCacheNumShardBits(-1)
                .setFilter(new BloomFilter());

        options.setTableFormatConfig(tableOptions);
        options.setWriteBufferSize(512 * SizeUnit.MB);
        options.setMaxWriteBufferNumber(5);
        options.setMinWriteBufferNumberToMerge(2);
        options.setMaxBackgroundCompactions(1);
        options.setMaxBackgroundFlushes(1);
        options.getEnv().setBackgroundThreads(8, RocksEnv.COMPACTION_POOL);
        options.getEnv().setBackgroundThreads(8, RocksEnv.FLUSH_POOL);
        options.setMaxWriteBufferNumber(8);
        options.setIncreaseParallelism(8);
        options.setWriteBufferSize(8 * SizeUnit.KB);
        options.setOptimizeFiltersForHits(true);
        options.prepareForBulkLoad();



//        options.setMaxOpenFiles(500000);
        //options.setDisableDataSync(false);//good for initial load

//        options.setTableCacheNumshardbits(6);
//        options.setAllowMmapReads(false);
//        options.setAllowMmapWrites(false);
        options.setBloomLocality(2);
        options.setMemtablePrefixBloomBits(10);
        options.setNumLevels(6);
//        options.setTargetFileSizeBase(67108864);
//        options.setTargetFileSizeMultiplier(1);
//        options.setMaxBytesForLevelBase(536870912);
//        options.setMaxBytesForLevelMultiplier(10);
//        options.setLevelZeroStopWritesTrigger(12);
//        options.setLevelZeroSlowdownWritesTrigger(8);
//        options.setLevelZeroFileNumCompactionTrigger(4);
//        options.setSoftRateLimit(0.0);
//        options.setHardRateLimit(1000);
//        options.setRateLimitDelayMaxMilliseconds(1000);
//        options.setMaxGrandparentOverlapFactor(10);
//        options.setDisableAutoCompactions(false);
//        options.setSourceCompactionFactor(1);
//        options.setFilterDeletes(false);
//        options.setMaxSuccessiveMerges(0);
//        options.setWalTtlSeconds(0);
//        options.setWalSizeLimitMB(0);


        options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
        options.setCompactionStyle(CompactionStyle.UNIVERSAL);

        options.setCreateIfMissing(true);
        options.setMergeOperatorName("put");
//                .getEnv()
//                .setBackgroundThreads(5, RocksEnv.COMPACTION_POOL)
//                .setBackgroundThreads(5, RocksEnv.FLUSH_POOL);

//                .createStatistics()
//                .setWriteBufferSize(8 * SizeUnit.KB)
//                .setMaxWriteBufferNumber(3)
//                .setMaxBackgroundCompactions(10)

//                .setEnv(rocksEnv);

        dataDBs = createDataDBs(spaceName, dbsNumber, dbPath, warm);
        metadataDB = createMetaDataDB(spaceName, dbPath, warm);

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
        return objectType == BlobStoreObjectType.DATA ? add(id, data, dataDBs.get(getDBNumberFromId(id))) :
                add(id, data, metadataDB);
    }

    private Object add(Serializable id, Serializable data, RocksDB rocksDB) {
        try{
            rocksDB.put(id.toString().getBytes(), (byte[])data);
        }catch (RocksDBException e){
            throw new BlobStoreException(e);
        }
        return -1;
    }


    @Override
    public Serializable get(Serializable id, Object position, BlobStoreObjectType objectType) {
        return objectType == BlobStoreObjectType.DATA ? get(id, position, dataDBs.get(getDBNumberFromId(id))) :
                get(id, position, metadataDB);
    }

    private Serializable get(Serializable id, Object position, RocksDB rocksDB) {
        byte[] data;
        try {
            data = rocksDB.get(id.toString().getBytes());
        }catch (RocksDBException e){
            throw new BlobStoreException(e);
        }
        if (data != null && data.length > 0){
            return data;
        }else{
            return null;
        }
    }



    @Override
    public Object replace(Serializable id, Serializable data, Object position, BlobStoreObjectType objectType) {
        return objectType == BlobStoreObjectType.DATA ? replace(id, data, position, dataDBs.get(getDBNumberFromId(id))) :
                replace(id, data, position, metadataDB);
    }


    private Object replace(Serializable id,Serializable data,  Object position, RocksDB rocksDB) {
        try {
            rocksDB.merge(id.toString().getBytes(), (byte[]) data);
        }catch (RocksDBException e){
            throw new BlobStoreException(e);
        }
        return -1;
    }


    @Override
    public Serializable remove(Serializable id, Object position, BlobStoreObjectType objectType) {
        if (objectType == BlobStoreObjectType.DATA)
            return remove(id, position, dataDBs.get(getDBNumberFromId(id)));
        else
            return remove(id, position, metadataDB);
    }

    private Serializable remove(Serializable id, Object position, RocksDB rocksDB) {
        try {
            rocksDB.remove(id.toString().getBytes());
        }catch (RocksDBException e){
            throw new BlobStoreException(e);
        }
        return null;
    }

    @Override
    public List<BlobStoreBulkOperationResult> executeBulk(List<BlobStoreBulkOperationRequest> operations, BlobStoreObjectType objectType, boolean transactional) {
        List<BlobStoreBulkOperationResult> blobStoreBulkOperationResults = null;

        if(transactional)
            blobStoreBulkOperationResults = super.executeBulk(operations, objectType, transactional);
        else{
            blobStoreBulkOperationResults = executeNonTransactionalBulk(operations);
        }
        return blobStoreBulkOperationResults;
    }

    @Override
    public DataIterator<BlobStoreGetBulkOperationResult> iterator(BlobStoreObjectType objectType) {
        if(objectType.equals(BlobStoreObjectType.METADATA)){
            return new RocksDBContainerIteratorMultiDBs(this, objectType);
        }
        else{
            return getIterator(objectType);
        }
    }

    @Override
    public void close() {
        for(RocksDB db : dataDBs){
            if(db != null)
                db.close();
        }
        if(metadataDB != null)
            metadataDB.close();
    }

    private synchronized List<RocksDB> createDataDBs(String spaceName, int dbCount, String dbPath, boolean warm){
        List<RocksDB> dbs = new ArrayList<RocksDB>();
        try{
            for(int i = 0; i < dbCount; i++) {
                if(!warm)
                    Utils.runCommand("mkdir -p " + new File(dbPath + "/" + spaceName +"_gs_" + i), new File("."));
                dbs.add(RocksDB.open(options, dbPath + "/" + spaceName + "_gs_" + i));
            }
            return dbs;
        }catch (RocksDBException e){
            throw new BlobStoreException(e);
        }
    }

    private synchronized RocksDB createMetaDataDB(String spaceName, String dbPath, boolean warm){
        try{
            Options options = new Options();
            if(!warm)
                Utils.runCommand("mkdir -p " + new File(dbPath + "/" + spaceName +"_metadata"), new File("."));
            return RocksDB.open(options.setCreateIfMissing(true), dbPath + "/" + spaceName + "_metadata");
        }catch (RocksDBException e){
            throw new BlobStoreException(e);
        }
    }

    private int getDBNumberFromId(Serializable id){
        return (Math.abs(id.hashCode())) % dbsNumber;
    }

    public RocksIterator createDataRocksIterator(int containerIndex){
        return dataDBs.get(containerIndex).newIterator();
    }

    public RocksIterator createMetadataRocksIterator(){
        return metadataDB.newIterator();
    }

    public RocksDBParallelDataContainerIteratorMultiDBs getIterator(BlobStoreObjectType objectType) {
        return new RocksDBParallelDataContainerIteratorMultiDBs(this, dbsNumber, objectType);
    }

    private List<BlobStoreBulkOperationResult> executeNonTransactionalBulk(List<BlobStoreBulkOperationRequest> operations) {
        List<List<BlobStoreBulkOperationRequest>> containersOps = new ArrayList<List<BlobStoreBulkOperationRequest>>();
        Map<BlobStoreBulkOperationRequest, BlobStoreBulkOperationResult> idRequestsMap = new ConcurrentHashMap<BlobStoreBulkOperationRequest, BlobStoreBulkOperationResult>();
        List<BlobStoreBulkOperationResult> results = new LinkedList<BlobStoreBulkOperationResult>();

        for(int i = 0 ; i < dbsNumber; i++){
            containersOps.add(null);
        }

        try{
            setOperationsPerDB(operations, containersOps, idRequestsMap);
            List<Future<?>> futures = new ArrayList<Future<?>>();
            for(int i = 0 ; i < dbsNumber; i++) {
                if(containersOps.get(i) == null)
                    continue;
                List<BlobStoreBulkOperationRequest> containerOps = containersOps.get(i);
                // start threads that perform batch operations on each container
                futures.add(bulkThreadPool.submit(new BlobStoreContainerBulkOperationTask(containerOps, idRequestsMap, i)));
            }
            for (Future<?> future : futures){
                try{
                    future.get();
                }
                catch(Exception e){
                    throw new BlobStoreException(e);
                }
            }
            gatherResultsFromOperations(operations, idRequestsMap, results);

        }catch (Exception e){
            throw new BlobStoreException(e);
        }
        return results;
    }

    private void setOperationsPerDB(List<BlobStoreBulkOperationRequest> operations, List<List<BlobStoreBulkOperationRequest>> dbsOps, Map<BlobStoreBulkOperationRequest, BlobStoreBulkOperationResult> idRequestsMap) {
        for(int i = 0; i < operations.size(); i++){
            int dbIndex = getDBNumberFromId(operations.get(i).getId());
            if(dbsOps.get(dbIndex) == null){
                dbsOps.set(dbIndex, new LinkedList<BlobStoreBulkOperationRequest>());
            }
            dbsOps.get(dbIndex).add(operations.get(i));
            BlobStoreBulkOperationResult res = idRequestsMap.put(operations.get(i), nullIndicator);
            if(res != null || nullIndicator.equals(res)){
                throw new BlobStoreException("Internal RocksDBHandler error");
            }
        }
    }

    private void gatherResultsFromOperations(List<BlobStoreBulkOperationRequest> operations, Map<BlobStoreBulkOperationRequest, BlobStoreBulkOperationResult> idRequestsMap, List<BlobStoreBulkOperationResult> results) {
        for(BlobStoreBulkOperationRequest request : operations){
            BlobStoreBulkOperationResult res = idRequestsMap.get(request);
            if(nullIndicator.equals(res))
                throw new BlobStoreException("Inconsistent results returned from ZetaScale. object id [ " + request.getId() +" ]");
            results.add(res);
        }
    }

    private void convertToBlobStoreAndPutResults( List<BlobStoreBulkOperationRequest> containerOps, Map<BlobStoreBulkOperationRequest, BlobStoreBulkOperationResult> opsIdsMap){
        int i = 0;
        for(BlobStoreBulkOperationRequest containerOp : containerOps){
            BlobStoreException exception = null;
            if(containerOp.getOpType().ordinal() == BlobStoreBulkOperationType.ADD.ordinal()) {
                String reqId = containerOps.get(i).getId().toString();
                opsIdsMap.put(containerOps.get(i), new BlobStoreAddBulkOperationResult(reqId, exception)); //check if not null
            }
            if(containerOp.getOpType().ordinal() == BlobStoreBulkOperationType.REMOVE.ordinal()) {
                String reqId = containerOps.get(i).getId().toString();
                opsIdsMap.put(containerOps.get(i), new BlobStoreRemoveBulkOperationResult(reqId, exception)); //check if not null
            }
            if(containerOp.getOpType().ordinal() == BlobStoreBulkOperationType.REPLACE.ordinal()) {
                String reqId = containerOps.get(i).getId().toString();
                opsIdsMap.put(containerOps.get(i), new BlobStoreReplaceBulkOperationResult(reqId, exception)); //check if not null
            }
            i++;
        }
    }

    private class BlobStoreContainerBulkOperationTask implements Runnable {

        private final List<BlobStoreBulkOperationRequest> dbOps;
        private final int dbIndex;
        private final Map<BlobStoreBulkOperationRequest, BlobStoreBulkOperationResult> idRequestsMap;

        public BlobStoreContainerBulkOperationTask(List<BlobStoreBulkOperationRequest> dbOps,
                                                   Map<BlobStoreBulkOperationRequest, BlobStoreBulkOperationResult> idRequestsMap, int dbIndex) {
            this.dbOps = dbOps;
            this.dbIndex = dbIndex;
            this.idRequestsMap = idRequestsMap;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("GS-Execute Bulk Pool#"+Thread.currentThread().getId());
            int size = dbOps.size();
            WriteBatch wb = new WriteBatch();
            if(dbOps.get(0).getOpType().equals(BlobStoreBulkOperationType.REMOVE)){
                for(int i = 0; i < size; i++) {
                    wb.remove(dbOps.get(i).getId().toString().getBytes());
                }
            }if(dbOps.get(0).getOpType().equals(BlobStoreBulkOperationType.ADD) || dbOps.get(0).getOpType().equals(BlobStoreBulkOperationType.REPLACE)){
                for(int i = 0; i < size; i++) {
                    if(dbOps.get(i).equals(BlobStoreBulkOperationType.ADD)) {
                        wb.put(dbOps.get(i).getId().toString().getBytes(), (byte[]) dbOps.get(i).getData());
                    }else{
                        wb.merge(dbOps.get(i).getId().toString().getBytes(), (byte[]) dbOps.get(i).getData());
                    }
                }
            }
            try {
                dataDBs.get(dbIndex).write(new WriteOptions(), wb);
            } catch (RocksDBException e) {
                throw new BlobStoreException(e);
            }

            convertToBlobStoreAndPutResults(dbOps, idRequestsMap);

        }
    }

    public void setDbPath(String dbPath) {
        this.dbPath = dbPath;
    }
}

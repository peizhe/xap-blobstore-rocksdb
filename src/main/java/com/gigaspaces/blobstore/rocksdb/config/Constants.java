package com.gigaspaces.blobstore.rocksdb.config;

/**
 * @author kobi on 5/5/15.
 * @since 10.2
 */
public interface Constants {

    final public static String STORAGE_HANDLER = "com.gigaspaces.com.gigaspaces.blobstore.storagehandler";

    static final String ROCKSDB_COLUMNS = "com.gs.com.gigaspaces.blobstore.rocksdb.columns";
    static final int ROCKSDB_COLUMNS_DEFAULT = 10;
    /**
     * The minimum number of threads of the pool that executes the bulk operations
     */
    public static final String BLOBSTORE_BULK_POOL_MIN_THREADS = "com.gs.com.gigaspaces.blobstore.bulkPoolMinThreads";

    /**
     * The default minimum number of threads of the pool that executes the bulk operations
     */
    public static final int BLOBSTORE_BULK_POOL_MIN_THREADS_DEFAULT = 4;

    /**
     * The maximum number of threads of the pool that executes the bulk operations
     */
    public static final String BLOBSTORE_BULK_POOL_MAX_THREADS = "com.gs.com.gigaspaces.blobstore.bulkPoolMaxThreads";

    /**
     * The default maximum number of threads of the pool that executes the bulk operations
     */
    public static final int BLOBSTORE_BULK_POOL_MAX_THREADS_DEFAULT = 30;
}

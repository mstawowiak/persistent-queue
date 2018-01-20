package com.github.mstawowiak.persistent.queue;

import com.github.mstawowiak.persistent.queue.exception.EnqueueException;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import java.io.File;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.concurrent.Semaphore;

public class BerkeleyDbQueue<P extends Payload> implements Queue<P> {

    /**
     * Berkley DB environment
     */
    private final Environment dbEnvironment;

    /**
     * Berkley DB instance for the queue
     */
    private final Database queueDatabase;

    /**
     * Name of this queue
     */
    private final String queueName;

    /**
     * Number of payloads used during flush it is allowed to loose in case of system crash.
     */
    private final int batchSize;

    /**
     * Queue operation counter, which is used to sync the queue database to disk periodically.
     */
    private int opsCounter;

    /**
     * Semaphore provides blocking queue future
     */
    protected final Semaphore semaphore;

    public BerkeleyDbQueue(final String queueEnvPath, final String queueName) {
        this(queueEnvPath, queueName, 1);
    }

    public BerkeleyDbQueue(final String queueEnvPath, final String queueName, final int batchSize) {
        // Create parent dirs for queue environment directory
        new File(queueEnvPath).mkdirs();

        // Setup database environment
        final EnvironmentConfig dbEnvConfig = new EnvironmentConfig();
        dbEnvConfig.setTransactional(false);
        dbEnvConfig.setAllowCreate(true);
        this.dbEnvironment = new Environment(new File(queueEnvPath), dbEnvConfig);

        // Setup non-transactional deferred-write queue database
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setDeferredWrite(true);
        dbConfig.setBtreeComparator(new KeyComparator());

        this.queueDatabase = dbEnvironment.openDatabase(null, queueName, dbConfig);
        this.semaphore = new Semaphore((int) queueDatabase.count());
        this.queueName = queueName;
        this.batchSize = batchSize;
        this.opsCounter = 0;
    }

    private static class KeyComparator implements Comparator<byte[]>, Serializable {

        @Override
        public int compare(byte[] key1, byte[] key2) {
            return new BigInteger(key1).compareTo(new BigInteger(key2));
        }

    }

    @Override
    public synchronized void push(Payload payload) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor cursor = queueDatabase.openCursor(null, null);
        try {
            cursor.getLast(key, data, LockMode.RMW);

            BigInteger prevKeyValue;
            if (key.getData() == null) {
                prevKeyValue = BigInteger.valueOf(-1);
            } else {
                prevKeyValue = new BigInteger(key.getData());
            }
            BigInteger newKeyValue = prevKeyValue.add(BigInteger.ONE);

            final DatabaseEntry newKey = new DatabaseEntry(newKeyValue.toByteArray());
            final DatabaseEntry newData = new DatabaseEntry(payload.serialize());
            queueDatabase.put(null, newKey, newData);

            opsCounter++;
            if (opsCounter >= batchSize) {
                queueDatabase.sync();
                opsCounter = 0;
            }

            semaphore.release();
        } catch (Exception ex) {
            throw new EnqueueException("Unable to enqueue payload", ex);
        } finally {
            cursor.close();
        }
    }

    @Override
    public P poll() {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final Cursor cursor = queueDatabase.openCursor(null, null);
        try {
            cursor.getFirst(key, data, LockMode.RMW);
            if (data.getData() == null) {
                return null;
            }
            final P payload = Payload.deserialize(data.getData());
            cursor.delete();
            opsCounter++;
            if (opsCounter >= batchSize) {
                queueDatabase.sync();
                opsCounter = 0;
            }
            return payload;
        } finally {
            cursor.close();
        }
    }

    @Override
    public P remove() {
        P payload = poll();
        if (payload != null) {
            return payload;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public P peek() {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final Cursor cursor = queueDatabase.openCursor(null, null);
        try {
            cursor.getFirst(key, data, LockMode.RMW);
            if (data.getData() == null) {
                return null;
            }
            return Payload.deserialize(data.getData());
        } finally {
            cursor.close();
        }
    }

    @Override
    public P element() {
        P payload = peek();
        if (payload != null) {
            return payload;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public long size() {
        return queueDatabase.count();
    }

    @Override
    public boolean isEmpty() {
        return queueDatabase.count() == 0;
    }

    @Override
    public String name() {
        return queueName;
    }

    @Override
    public void close() {
        queueDatabase.close();
        dbEnvironment.close();
    }

    @Override
    public Semaphore getSemaphore() {
        return semaphore;
    }

}

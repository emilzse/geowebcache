/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * @author Emil Zail built on SeedTask
 */
package org.geowebcache.rest.controller.seed.invalidate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.conveyor.ConveyorTile;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.wms.WMSLayer;
import org.geowebcache.rest.controller.seed.invalidate.pojos.DeletedTile;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.storage.StorageBroker;
import org.geowebcache.util.Sleeper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.annotations.VisibleForTesting;

/**
 * A GWCTask for validating tiles
 *
 */
class ValidateTask extends GWCTask {
    
//    public static void main(String[] args){
//        
//    }
    
    private static Log log = LogFactory.getLog(org.geowebcache.rest.controller.seed.invalidate.ValidateTask.class);

    private StorageBroker storageBroker;

    private int tileFailureRetryCount;

    private long tileFailureRetryWaitTime;

    private long totalFailuresBeforeAborting;

    private AtomicLong sharedFailureCounter;
    
    private final TileLayer tl;
    
    private final DataSource dataSource;
    
    /**
     * The template used to execute commands
     */
    private final NamedParameterJdbcTemplate jt;

    private final String tableName;
    
    private final String jobId;
    
    private final int poolSize;
    
    /**
     * How many invalidated tiles to generate at a time
     */
    private static final int BATCH_SIZE = 50;

    @VisibleForTesting
    Sleeper sleeper = Thread::sleep;
    
    /**
     * Constructs a InvalidateTask
     * 
     * @param storageBroker
     * @param quotaStore
     * @param tl
     * @param maxPageZ restrict generating tiles up to this page level (zoom/scale-level)
     */
    public ValidateTask(StorageBroker storageBroker, TileLayer tl, DataSource dataSource, String tableName, String jobId, int poolSize) {
        this.storageBroker = storageBroker;
        this.tl = tl;
        this.layerName = tl.getName();
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.jobId = jobId;
        this.poolSize = poolSize < 1 ? 1 : poolSize;
        
        DataSourceTransactionManager dsTransactionManager = new DataSourceTransactionManager(
                this.dataSource);
        this.jt = new NamedParameterJdbcTemplate(dsTransactionManager.getDataSource());

        tileFailureRetryCount = 0;
        tileFailureRetryWaitTime = 100;
        totalFailuresBeforeAborting = 10000;
        sharedFailureCounter = new AtomicLong();

        super.parsedType = GWCTask.TYPE.VALIDATE;

        super.state = GWCTask.STATE.READY;
    }

    @Override
    protected void doActionInternal() throws GeoWebCacheException, InterruptedException {
        super.state = GWCTask.STATE.RUNNING;

        // Lower the priority of the thread
        reprioritize();

        checkInterrupted();

        // approximate thread creation time
        final long START_TIME = System.currentTimeMillis();

        log.info(getThreadName() + " begins validating tiles for " + layerName + " with pool size " + poolSize);

        checkInterrupted();

        super.tilesTotal = getCount(jobId);

        checkInterrupted();

        List<DeletedTile> list = null;
        
        // run job in thread pool
        ExecutorService exec = Executors.newFixedThreadPool(poolSize);
        try {
            while ((list = getList(jobId, BATCH_SIZE)).size() > 0 && this.terminate == false) {
                checkInterrupted();
                List<Future<?>> futures = new ArrayList<Future<?>>(list.size());
                for (DeletedTile tile : list) {
                    futures.add(exec.submit(new Processor(tile, START_TIME)));
                }
                
                for (Future<?> f : futures) {
                    try {
                        f.get(); // wait for a processor to complete
                    } catch (InterruptedException | ExecutionException e) {
                        log.warn("Failed execution", e);
                    } 
                }
                
                // delete list
                deleteInvalidated(jobId, BATCH_SIZE);
    
                checkInterrupted();
            }
        } finally {
            exec.shutdown(); 
        }

        if (this.terminate) {
            log.info("Job on " + getThreadName() + " was terminated after "
                    + this.tilesDone + " tiles");
        } else {
            // delete rest
            deleteInvalidated(jobId, BATCH_SIZE);

            log.info(getThreadName() + " completed " + parsedType.toString()
                    + " layer " + layerName
                    + " after " + this.tilesDone + " tiles and " + this.timeSpent + " seconds.");
        }

        checkInterrupted();

        super.state = GWCTask.STATE.DONE;
    }

    private void reprioritize() {
        Thread.currentThread().setPriority(
                (java.lang.Thread.NORM_PRIORITY + java.lang.Thread.MIN_PRIORITY) / 2);
    }

    private void waitToRetry() throws InterruptedException {
        sleeper.sleep(tileFailureRetryWaitTime);
    }

    private String getThreadName() {
        return Thread.currentThread().getName();
    }

    /**
     * Helper method to update the members tracking thread progress.
     * 
     * @param layer
     * @param zoomStart
     * @param zoomStop
     * @param level
     * @param gridBounds
     * @return
     */
    private void updateStatusInfo(TileLayer layer, long start_time) {

        // working on tile
        this.tilesDone.incrementAndGet();

        // estimated time of completion in seconds, use a moving average over the last
        this.timeSpent = (int) (System.currentTimeMillis() - start_time) / 1000;

        int threadCount = sharedThreadCount.get();
        long timeTotal = Math.round((double) timeSpent
                * (((double) tilesTotal / threadCount) / (double) this.tilesDone.get()));

        this.timeRemaining = (int) (timeTotal - timeSpent);
    }

    public void setFailurePolicy(int tileFailureRetryCount, long tileFailureRetryWaitTime,
            long totalFailuresBeforeAborting, AtomicLong sharedFailureCounter) {
        this.tileFailureRetryCount = tileFailureRetryCount;
        this.tileFailureRetryWaitTime = tileFailureRetryWaitTime;
        this.totalFailuresBeforeAborting = totalFailuresBeforeAborting;
        this.sharedFailureCounter = sharedFailureCounter;
    }

    @Override
    protected void dispose() {
        if (tl instanceof WMSLayer) {
            ((WMSLayer) tl).cleanUpThreadLocals();
        }
    }
    
    private void deleteInvalidated(String jobId, int limit) {
       String deleteList = String.format("DELETE FROM %s WHERE id IN (SELECT id FROM %s WHERE group_id = :jobId ORDER BY id ASC LIMIT %d)", tableName, tableName, limit);

       if (log.isDebugEnabled()) {
           log.info("Delete list of tiles: jobId=" + jobId + " sql=" + deleteList);
       }
      
       jt.update(deleteList, Collections.singletonMap("jobId", jobId));
    }
    
    private long getCount(String jobId) {
       String getCount = String.format("SELECT count(*) FROM %s WHERE group_id = :jobId", tableName);

       if (log.isDebugEnabled()) {
           log.info("Count invalidated tiles: jobId=" + jobId + " sql=" + getCount);
       }
                
       return jt.queryForObject(getCount, Collections.singletonMap("jobId", jobId), Long.class);
    }
    
    private List<DeletedTile> getList(String jobId, int limit) {
        String getList = String.format("SELECT * FROM %s WHERE group_id = :jobId ORDER BY id ASC LIMIT %d", tableName, limit);
        
         if (log.isDebugEnabled()) {
             log.info("Get list of tiles: jobId=" + jobId + " sql=" + getList);
         }
         
         return jt.query(getList, Collections.singletonMap("jobId", jobId), new RowMapper<DeletedTile>() {
                   
                   public DeletedTile mapRow(ResultSet rs, int rowNum) throws SQLException {
                     // group_id, layer_name, gridset_id, x, y, z, mime_type, parameters_kvp
                     return new DeletedTile(rs.getString("layer_name"), rs.getString("gridset_id"), rs.getLong("x"), rs.getLong("y"), rs.getLong("z"), rs.getString("mime_type"), getParameters(rs.getString("parameters_kvp")));
                   }
             });
    }
    
    private Map<String,String> getParameters(String parametersKvp) {
        Map<String, String> parameters = null;
        if (parametersKvp != null && !parametersKvp.isEmpty()) {
            parameters = Pattern.compile("&")
                    .splitAsStream(parametersKvp.replace("?", ""))
                    .map(p -> p.split("="))
                    .collect(Collectors.toMap(s -> s[0], s -> s.length > 1 ? s[1] : ""));
        }
        
        return parameters;
    }
    
    private void setState(GWCTask.STATE state){
        super.state = state;
    }
    
    private boolean checkTerminate(){
        return this.terminate;
    }
    
    private class Processor implements Runnable {

        private final long startTime;
        private final DeletedTile dt;
        
        public Processor(DeletedTile dt, long startTime) {
            super();
            this.dt = dt;
            this.startTime = startTime;
        }
        
        @Override
        public void run() {
            try {
                if (!checkTerminate()) {
                    checkInterrupted();
    
                    ConveyorTile tile = dt.createTile(storageBroker);
                    
                    for (int fetchAttempt = 0; fetchAttempt <= tileFailureRetryCount; fetchAttempt++) {
                        try {
                            checkInterrupted();
                            tl.seedTile(tile, true, false);
                            break;// success, let it go
                        } catch (Exception e) {
                            // if GWC_SEED_RETRY_COUNT was not set then none of the settings have effect, in
                            // order to keep backwards compatibility with the old behaviour
                            if (tileFailureRetryCount == 0) {
                                if (e instanceof GeoWebCacheException) {
                                    throw (GeoWebCacheException) e;
                                }
                                throw new GeoWebCacheException(e);
                            }
    
                            long sharedFailureCount = sharedFailureCounter.incrementAndGet();
                            if (sharedFailureCount >= totalFailuresBeforeAborting) {
                                log.info("Aborting seed thread " + getThreadName()
                                        + ". Error count reached configured maximum of "
                                        + totalFailuresBeforeAborting);
                                setState(GWCTask.STATE.DEAD);
                                return;
                            }
                            String logMsg = "Seed failed at " + tile.toString() + " after "
                                    + (fetchAttempt + 1) + " of " + (tileFailureRetryCount + 1)
                                    + " attempts.";
                            if (fetchAttempt < tileFailureRetryCount) {
                                log.debug(logMsg);
                                if (tileFailureRetryWaitTime > 0) {
                                    log.trace("Waiting " + tileFailureRetryWaitTime
                                            + " before trying again");
                                    waitToRetry();
                                }
                            } else {
                                log.info(logMsg
                                        + " Skipping and continuing with next tile. Original error: "
                                        + e.getMessage());
                            }
                        }
                    }
                    
                    // How many tiles completed
    
                    updateStatusInfo(tl, startTime);
    
                    checkInterrupted();
                }
           } catch(Exception e) {
               log.warn(e);
           }
        }
        
    }
}

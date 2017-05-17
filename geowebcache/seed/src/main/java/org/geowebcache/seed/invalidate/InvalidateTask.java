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
package org.geowebcache.seed.invalidate;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.diskquota.QuotaStore;
import org.geowebcache.diskquota.storage.TilePage;
import org.geowebcache.grid.BoundingBox;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.wms.WMSLayer;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.InvalidateConfig;
import org.geowebcache.storage.StorageBroker;
import org.geowebcache.storage.StorageException;
import org.geowebcache.storage.TileObject;
import org.geowebcache.util.Sleeper;

import com.google.common.annotations.VisibleForTesting;

/**
 * A GWCTask for invalidating tiles
 *
 */
class InvalidateTask extends GWCTask {
    private static Log log = LogFactory.getLog(org.geowebcache.seed.invalidate.InvalidateTask.class);

    private StorageBroker storageBroker;

    private QuotaStore quotaStore;

    private int tileFailureRetryCount;

    private long tileFailureRetryWaitTime;

    private long totalFailuresBeforeAborting;

    private AtomicLong sharedFailureCounter;
    
    private final TileLayer tl;

    private final InvalidateConfig[] invalidateList;

    @VisibleForTesting
    Sleeper sleeper = Thread::sleep;
    
    /**
     * Constructs a InvalidateTask
     * 
     * @param storageBroker
     * @param quotaStore
     * @param tl
     * @param list invalidate config
     */
    public InvalidateTask(StorageBroker storageBroker, QuotaStore quotaStore, TileLayer tl,
            InvalidateConfig[] list) {

        this.quotaStore = quotaStore;
        this.storageBroker = storageBroker;
        this.tl = tl;
        this.layerName = tl.getName();
        this.invalidateList = list;

        tileFailureRetryCount = 0;
        tileFailureRetryWaitTime = 100;
        totalFailuresBeforeAborting = 10000;
        sharedFailureCounter = new AtomicLong();

        super.parsedType = GWCTask.TYPE.INVALIDATE;

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

        log.info(getThreadName() + " begins invalidating tiles for " + layerName + ": invalidateConfigs="
                + invalidateList.length);

        checkInterrupted();

        super.tilesTotal = invalidateList.length;

        checkInterrupted();

        for (int i = 0; i < invalidateList.length && this.terminate == false; i++) {

            checkInterrupted();

            // get bbox for tile

            InvalidateConfig obj = invalidateList[i];
            
            BoundingBox tileBbox = obj.bounds;

            for (int fetchAttempt = 0; fetchAttempt <= tileFailureRetryCount; fetchAttempt++) {
                try {
                    checkInterrupted();

                    log.info("invalidate-item=" + obj);

                    // invalidate in db
                    quotaStore.invalidateTilePages(layerName, tileBbox, obj.epsgId, obj.scaleLevel);
                    
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
                        super.state = GWCTask.STATE.DEAD;
                        return;
                    }
                    String logMsg = "Invalidate failed at " + obj + " after "
                            + (fetchAttempt + 1) + " of " + (tileFailureRetryCount + 1)
                            + " attempts.";
                    if (fetchAttempt < tileFailureRetryCount) {
                        log.debug(logMsg);
                        if (tileFailureRetryWaitTime > 0) {
                            log.trace(
                                    "Waiting " + tileFailureRetryWaitTime + " before trying again");
                            waitToRetry();
                        }
                    } else {
                        log.info(
                                logMsg + " Skipping and continuing with next tile. Original error: "
                                        + e.getMessage());
                    }
                }
            }

            if (log.isTraceEnabled()) {
                log.trace(getThreadName() + " invalidated " + obj);
            }

            // i starts with 0
            final long tilesCompletedByThisThread = i + 1;

            updateStatusInfo(tl, tilesCompletedByThisThread, START_TIME);

            checkInterrupted();
        }

        if (!this.terminate) {
            checkInterrupted();

            // remove tiles from file system
            List<TilePage> pages = quotaStore.getInvalidatedTilePages(layerName, false);

            try {
                log.info("Invalidated pages, will delete tiles: count=" + pages.size());

                storageBroker.delete(
                        pages.stream().map(this::initFromTilePage).collect(Collectors.toList()));
                
                // Mark as deleted
                quotaStore.setDeletedInvalidatedTilePages(layerName);
            } catch (StorageException e) {
                log.error("Failed to delete invalidated tiles: msg=" + e.getMessage());

                throw new GeoWebCacheException(e);
            }
        }

        if (this.terminate) {
            log.info("Job on " + getThreadName() + " was terminated after "
                    + this.tilesDone + " tiles");
        } else {
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
    private void updateStatusInfo(TileLayer layer, long tilesCount, long start_time) {

        // working on tile
        this.tilesDone = tilesCount;

        // estimated time of completion in seconds, use a moving average over the last
        this.timeSpent = (int) (System.currentTimeMillis() - start_time) / 1000;

        int threadCount = sharedThreadCount.get();
        long timeTotal = Math.round((double) timeSpent
                * (((double) tilesTotal / threadCount) / (double) tilesCount));

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

    /**
     * Initializes the other fields of the tilepage from an id with the
     * layer#gridset#format[#paramId] structure
     */
    private TileObject initFromTilePage(TilePage page) {
        String[] splitted = page.getTileSetId().split("#");
        if (splitted.length < 3 || splitted.length > 4) {
            throw new IllegalArgumentException("Invalid key for standard tile page, "
                    + "it should have the layer#gridset#format[#paramId]");
        }
        String gridsetId = splitted[1];

        return TileObject.createQueryTileObject(splitted[0], page.getTileIndex(), gridsetId,
                splitted[2], splitted.length == 4 ? splitted[3] : null);
    }
}

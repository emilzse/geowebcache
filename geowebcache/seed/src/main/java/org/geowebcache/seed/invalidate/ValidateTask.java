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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.conveyor.ConveyorTile;
import org.geowebcache.diskquota.QuotaStore;
import org.geowebcache.diskquota.storage.TilePage;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.wms.WMSLayer;
import org.geowebcache.mime.MimeException;
import org.geowebcache.mime.MimeType;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.storage.StorageBroker;
import org.geowebcache.storage.TileObject;
import org.geowebcache.storage.TileRange;
import org.geowebcache.storage.TileRangeIterator;
import org.geowebcache.util.Sleeper;

import com.google.common.annotations.VisibleForTesting;

/**
 * A GWCTask for validating tiles
 *
 */
class ValidateTask extends GWCTask {
    private static Log log = LogFactory.getLog(org.geowebcache.seed.invalidate.ValidateTask.class);

    private StorageBroker storageBroker;

    private QuotaStore quotaStore;

    private int tileFailureRetryCount;

    private long tileFailureRetryWaitTime;

    private long totalFailuresBeforeAborting;

    private AtomicLong sharedFailureCounter;
    
    private final TileLayer tl;

    @VisibleForTesting
    Sleeper sleeper = Thread::sleep;
    
    /**
     * Constructs a InvalidateTask
     * 
     * @param storageBroker
     * @param quotaStore
     * @param tl
     */
    public ValidateTask(StorageBroker storageBroker, QuotaStore quotaStore, TileLayer tl) {

        this.quotaStore = quotaStore;
        this.storageBroker = storageBroker;
        this.tl = tl;
        this.layerName = tl.getName();

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

        log.info(getThreadName() + " begins validating tiles for " + layerName);

        checkInterrupted();

        final int metaTilingFactorX = tl.getMetaTilingFactors()[0];
        final int metaTilingFactorY = tl.getMetaTilingFactors()[1];
        
        List<TilePage> pages = quotaStore.getInvalidatedTilePages(layerName, true);
        
        super.tilesTotal = pages.size();

        checkInterrupted();

        // long seedCalls = 0;
        for (int i = 0; i < pages.size() && this.terminate == false; i++) {

            checkInterrupted();

            TilePage page = pages.get(i);
            
            // Will check if time to live is expired
            if (!controlTTL(page)) {
                log.info("TLL expired, will delete record: page=" + page.getKey());
                
                quotaStore.deleteTilePage(page);
            } else {
                // Create ConveyorTile (s) from TilePage and seed tile (s)
    
                TileRange tr = initFromTilePage(page);
                
                TileRangeIterator trIter = new TileRangeIterator(tr, tl.getMetaTilingFactors());
                
                long[] gridLoc = trIter.nextMetaGridLocation(new long[3]);

                long seedCalls = 0;
                while (gridLoc != null && this.terminate == false) {

                    checkInterrupted();
                    Map<String, String> fullParameters = tr.getParameters();

                    ConveyorTile tile = new ConveyorTile(storageBroker, layerName, tr.getGridSetId(), gridLoc,
                            tr.getMimeType(), fullParameters);

                    for (int fetchAttempt = 0; fetchAttempt <= tileFailureRetryCount; fetchAttempt++) {
                        try {
                            checkInterrupted();
                            tl.seedTile(tile, false, false);
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

                    if (log.isTraceEnabled()) {
                        log.trace(getThreadName() + " seeded " + Arrays.toString(gridLoc));
                    }

                    // final long totalTilesCompleted = trIter.getTilesProcessed();
                    // note: computing the # of tiles processed by this thread instead of by the whole group
                    // also reduces thread contention as the trIter methods are synchronized and profiler
                    // shows 16 threads block on synchronization about 40% the time
                    final long tilesCompletedByThisThread = seedCalls * metaTilingFactorX
                            * metaTilingFactorY;

                    updateStatusInfo(tl, tilesCompletedByThisThread, START_TIME);

                    checkInterrupted();
                    seedCalls++;
                    gridLoc = trIter.nextMetaGridLocation(gridLoc);
                }

            }
            // i starts with 0
            final long tilesCompletedByThisThread = i + 1;

            updateStatusInfo(tl, tilesCompletedByThisThread, START_TIME);

            checkInterrupted();
        }

        if (this.terminate) {
            log.info("Job on " + getThreadName() + " was terminated after "
                    + this.tilesDone + " tiles");
        } else {
            // validate all tiles
            quotaStore.validateTilePages(layerName);

            log.info(getThreadName() + " completed " + parsedType.toString()
                    + " layer " + layerName
                    + " after " + this.tilesDone + " tiles and " + this.timeSpent + " seconds.");
        }

        checkInterrupted();

        super.state = GWCTask.STATE.DONE;
    }

    /**
     * @param page
     * @return if time-to-live have expired
     */
    private boolean controlTTL(TilePage page) {
        boolean seedTile = true;
        
        // if has a expire cache for zoom level
        int expireCache = tl.getExpireCache(page.getZoomLevel());
        if (expireCache > 0) {
            long expireCacheTime = expireCache * 1000l;
            long creationTime = page.getCreationTimeMinutes() * 60l * 1000l;
            long currentTime = System.currentTimeMillis();
            
            if (currentTime - expireCacheTime > creationTime) {
                // to old, should delete
                seedTile = false;
            }
        }
        
        return seedTile;
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
    private TileRange initFromTilePage(TilePage page) {
        String[] splitted = page.getTileSetId().split("#");
        if (splitted.length < 3 || splitted.length > 4) {
            throw new IllegalArgumentException("Invalid key for standard tile page, "
                    + "it should have the layer#gridset#format[#paramId]");
        }
        String gridsetId = splitted[1];
        long[] pageCoverage = page.getPageCoverage();
        byte z =  page.getZoomLevel();
        long[][] rangeBounds = new long[z][];
        rangeBounds[z-1] = pageCoverage;
        
        Map<String, String> parameters = null;
        String parametersKvp = page.getParametersKvp();
        if (parametersKvp != null && !parametersKvp.isEmpty()) {
            parameters = Pattern.compile("&")
                    .splitAsStream(parametersKvp.replace("?", ""))
                    .map(p -> p.split("="))
                    .collect(Collectors.toMap(s -> s[0], s -> s.length > 1 ? s[1] : ""));
        }
        
        try {
            return new TileRange(splitted[0], gridsetId, z, z, rangeBounds, MimeType.createFromFormat(splitted[2]), parameters);
        } catch (MimeException e) {
            return null;
        }
    }
    
    /**
     * helper for counting the number of tiles
     * 
     * @param tr
     * @return -1 if too many
     */
    private long tileCount(TileRange tr) {

        final int startZoom = tr.getZoomStart();
        final int stopZoom = tr.getZoomStop();

        long count = 0;

        for (int z = startZoom; z <= stopZoom; z++) {
            long[] gridBounds = tr.rangeBounds(z);

            final long minx = gridBounds[0];
            final long maxx = gridBounds[2];
            final long miny = gridBounds[1];
            final long maxy = gridBounds[3];

            long thisLevel = (1 + maxx - minx) * (1 + maxy - miny);

            if (thisLevel > (Long.MAX_VALUE / 4) && z != stopZoom) {
                return -1;
            } else {
                count += thisLevel;
            }
        }

        return count;
    }
}

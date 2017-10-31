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
 * @author Marius Suta / The Open Planning Project 2008
 * @author Arne Kepp / The Open Planning Project 2009 
 */
package org.geowebcache.seed;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.conveyor.ConveyorTile;
import org.geowebcache.filter.request.RequestFilter;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.wms.WMSLayer;
import org.geowebcache.storage.StorageBroker;
import org.geowebcache.storage.TileRange;
import org.geowebcache.storage.TileRangeIterator;
import org.geowebcache.util.Sleeper;

import com.google.common.annotations.VisibleForTesting;

/**
 * A GWCTask for seeding/reseeding the cache.
 *
 */
class SeedTask extends GWCTask {
    private static Log log = LogFactory.getLog(org.geowebcache.seed.SeedTask.class);

    private final TileRangeIterator trIter;

    private final TileLayer tl;

    // private boolean reseed;

    private boolean doFilterUpdate;

    private StorageBroker storageBroker;

    private int tileFailureRetryCount;

    private long tileFailureRetryWaitTime;

    private long totalFailuresBeforeAborting;

    private AtomicLong sharedFailureCounter;
    
    @VisibleForTesting
    Sleeper sleeper = Thread::sleep;
    
    /**
     * Constructs a SeedTask
     * @param sb
     * @param trIter
     * @param tl
     * @param reseed
     * @param doFilterUpdate
     */
    public SeedTask(StorageBroker sb, TileRangeIterator trIter, TileLayer tl, GWCTask.TYPE seedType,
            boolean doFilterUpdate) {
        this.storageBroker = sb;
        this.trIter = trIter;
        this.tl = tl;
        this.doFilterUpdate = doFilterUpdate;

        tileFailureRetryCount = 0;
        tileFailureRetryWaitTime = 100;
        totalFailuresBeforeAborting = 10000;
        sharedFailureCounter = new AtomicLong();

        super.parsedType = seedType;
        super.layerName = tl.getName();

        super.state = GWCTask.STATE.READY;
    }

    // TODO: refactoring this into smaller functions might improve readability
    @Override
    protected void doActionInternal() throws GeoWebCacheException, InterruptedException {
        super.state = GWCTask.STATE.RUNNING;

        // Lower the priority of the thread
        reprioritize();

        checkInterrupted();

        // approximate thread creation time
        final long START_TIME = System.currentTimeMillis();

        final String layerName = tl.getName();
        log.info(getThreadName() + " begins seeding layer : " + layerName
                + " with seed type : " + parsedType);

        TileRange tr = trIter.getTileRange();

        checkInterrupted();

        // count tiles
        super.tilesTotal = tr.tileCount();

        final int metaTilingFactorX = tl.getMetaTilingFactors()[0];
        final int metaTilingFactorY = tl.getMetaTilingFactors()[1];

        final boolean tryCache = parsedType == GWCTask.TYPE.SEED;
        final boolean cacheOnly = parsedType == GWCTask.TYPE.RENEW;

        checkInterrupted();
        long[] gridLoc = trIter.nextMetaGridLocation(new long[3]);

        long seedCalls = 0;
        while (gridLoc != null && this.terminate == false) {

            checkInterrupted();
            Map<String, String> fullParameters = tr.getParameters();

            ConveyorTile tile = new ConveyorTile(storageBroker, layerName, tr.getGridSetId(), gridLoc,
                    tr.getMimeType(), fullParameters, null, null);

            for (int fetchAttempt = 0; fetchAttempt <= tileFailureRetryCount; fetchAttempt++) {
                try {
                    checkInterrupted();
                    tl.seedTile(tile, tryCache, cacheOnly);
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

        if (this.terminate) {
            log.info("Job on " + getThreadName() + " was terminated after "
                    + this.tilesDone.get() + " tiles");
        } else {
            log.info(getThreadName() + " completed " + parsedType.toString()
                    + " layer " + layerName
                    + " after " + this.tilesDone + " tiles and " + this.timeSpent + " seconds.");
        }

        checkInterrupted();
        if (threadOffset == 0 && doFilterUpdate) {
            runFilterUpdates(tr.getGridSetId());
        }

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
        this.tilesDone.set(tilesCount);

        // estimated time of completion in seconds, use a moving average over the last
        this.timeSpent = (int) (System.currentTimeMillis() - start_time) / 1000;

        int threadCount = sharedThreadCount.get();
        long timeTotal = Math.round((double) timeSpent
                * (((double) tilesTotal / threadCount) / (double) tilesCount));

        this.timeRemaining = (int) (timeTotal - timeSpent);
    }

    /**
     * Updates any request filters
     */
    private void runFilterUpdates(String gridSetId) {
        // We will assume that all filters that can be updated should be updated
        List<RequestFilter> reqFilters = tl.getRequestFilters();
        if (reqFilters != null && !reqFilters.isEmpty()) {
            Iterator<RequestFilter> iter = reqFilters.iterator();
            while (iter.hasNext()) {
                RequestFilter reqFilter = iter.next();
                if (reqFilter.update(tl, gridSetId)) {
                    log.info("Updated request filter " + reqFilter.getName());
                } else {
                    log.debug("Request filter " + reqFilter.getName()
                            + " returned false on update.");
                }
            }
        }
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
}

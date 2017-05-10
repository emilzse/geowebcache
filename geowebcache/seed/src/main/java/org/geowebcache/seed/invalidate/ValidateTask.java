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
import org.geowebcache.grid.BoundingBox;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.wms.WMSLayer;
import org.geowebcache.mime.MimeType;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.storage.StorageBroker;
import org.geowebcache.storage.TileObject;
import org.geowebcache.util.Sleeper;

import com.google.common.annotations.VisibleForTesting;

/**
 * A GWCTask for invalidating tiles from z/x/y calculating bbox by
 * http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Java
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
     * @param tl
     * @param gridSetId
     * @param zxy
     *            z/x/y
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

        List<TilePage> pages = quotaStore.getInvalidatedTilePages(layerName);

        super.tilesTotal = pages.size();

        checkInterrupted();

        // long seedCalls = 0;
        for (int i = 0; i < pages.size() && this.terminate == false; i++) {

            checkInterrupted();

            TilePage page = pages.get(i);

            // Create ConveyorTile from TilePage

            TileObject tileObj = initFromTilePage(page);

            ConveyorTile tile = new ConveyorTile(storageBroker, layerName, tileObj.getGridSetId(),
                    tileObj.getXYZ(), MimeType.createFromFormat(tileObj.getBlobFormat()),
                    tileObj.getParameters());

            tile.setTileLayer(tl);

            for (int fetchAttempt = 0; fetchAttempt <= tileFailureRetryCount; fetchAttempt++) {
                try {
                    checkInterrupted();

                    // seed tile
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
                log.trace(getThreadName() + " validated " + tile.toString());
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

    // OSM http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Java

    private BoundingBox tile2boundingBox(final int[] zxy) {
        int zoom = zxy[0];
        int x = zxy[1];
        int y = zxy[2];
        
        double maxY = tile2lat(y, zoom);
        double minY = tile2lat(y + 1, zoom);
        double minX = tile2lon(x, zoom);
        double maxX = tile2lon(x + 1, zoom);

        return new BoundingBox(minX, minY, maxX, maxY);
    }

    static double tile2lon(int x, int z) {
        return x / Math.pow(2.0, z) * 360.0 - 180;
    }

    static double tile2lat(int y, int z) {
        double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, z);
        return Math.toDegrees(Math.atan(Math.sinh(n)));
    }

    /**
     * Initializes the other fields of the tilepage from an id with the
     * layer#gridset#format[#paramId] structure
     */
    private static TileObject initFromTilePage(TilePage page) {
        String[] splitted = page.getTileSetId().split("#");
        if (splitted.length < 3 || splitted.length > 4) {
            throw new IllegalArgumentException("Invalid key for standard tile page, "
                    + "it should have the layer#gridset#format[#paramId]");
        }
        
        Map<String, String> parameters = null;
        String parametersKvp = page.getParametersKvp();
        if (parametersKvp != null && !parametersKvp.isEmpty()) {
            parameters = Pattern.compile("&")
                    .splitAsStream(parametersKvp.replace("?", ""))
                    .map(p -> p.split("="))
                    .collect(Collectors.toMap(s -> s[0], s -> s.length > 1 ? s[1] : ""));
        }
        
        return TileObject.createQueryTileObject(splitted[0], page.getTileIndex(), splitted[1],
                splitted[2], splitted.length == 4 ? splitted[3] : null, parameters);
    }
}

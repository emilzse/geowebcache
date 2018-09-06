/**
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * <p>You should have received a copy of the GNU Lesser General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 * @author Emil Zail built on SeedTask
 */
package org.geowebcache.rest.controller.seed.invalidate;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.diskquota.QuotaStore;
import org.geowebcache.diskquota.storage.TilePage;
import org.geowebcache.filter.parameters.ParametersUtils;
import org.geowebcache.grid.BoundingBox;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.wms.WMSLayer;
import org.geowebcache.mime.MimeException;
import org.geowebcache.mime.MimeType;
import org.geowebcache.rest.controller.seed.invalidate.pojos.DeletedTile;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.InvalidateConfig;
import org.geowebcache.storage.BlobStoreListener;
import org.geowebcache.storage.StorageBroker;
import org.geowebcache.storage.TileRange;
import org.geowebcache.util.Sleeper;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

/** A GWCTask for invalidating tiles */
class InvalidateTask extends GWCTask {
    private static Log log =
            LogFactory.getLog(org.geowebcache.rest.controller.seed.invalidate.InvalidateTask.class);

    private StorageBroker storageBroker;

    private QuotaStore quotaStore;

    private int tileFailureRetryCount;

    private long tileFailureRetryWaitTime;

    private long totalFailuresBeforeAborting;

    private AtomicLong sharedFailureCounter;

    private AtomicLong sharedDeletedCounter;

    private final TileLayer tl;

    private final InvalidateConfig[] invalidateList;

    private final BlockingQueue<DeletedTile> deletedTiles;

    private final DataSource dataSource;

    private final String tableName;

    private final String jobId;

    private final int poolSize;

    @VisibleForTesting Sleeper sleeper = Thread::sleep;

    /**
     * Constructs a InvalidateTask
     *
     * @param storageBroker
     * @param quotaStore
     * @param tl
     * @param list invalidate config
     */
    public InvalidateTask(
            StorageBroker storageBroker,
            QuotaStore quotaStore,
            TileLayer tl,
            InvalidateConfig[] list,
            DataSource dataSource,
            String tableName,
            String jobId,
            int poolSize) {

        this.quotaStore = quotaStore;
        this.storageBroker = storageBroker;
        this.tl = tl;
        this.layerName = tl.getName();
        this.invalidateList = list;
        this.deletedTiles = new LinkedBlockingQueue<DeletedTile>(1000);
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.jobId = jobId;
        this.poolSize = poolSize < 1 ? 1 : poolSize;

        tileFailureRetryCount = 0;
        tileFailureRetryWaitTime = 100;
        totalFailuresBeforeAborting = 10000;
        sharedFailureCounter = new AtomicLong();
        sharedDeletedCounter = new AtomicLong();

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

        log.info(
                getThreadName()
                        + " begins invalidating tiles for "
                        + layerName
                        + ": invalidateConfigs="
                        + invalidateList.length
                        + " with pool size "
                        + poolSize);

        checkInterrupted();

        super.tilesTotal = invalidateList.length;

        checkInterrupted();

        for (int i = 0; i < invalidateList.length && this.terminate == false; i++) {

            checkInterrupted();

            // get bbox for tile

            InvalidateConfig obj = invalidateList[i];

            BoundingBox invalidateBbox = obj.bounds;

            for (int fetchAttempt = 0; fetchAttempt <= tileFailureRetryCount; fetchAttempt++) {
                try {
                    checkInterrupted();

                    log.debug("invalidate-item=" + obj);

                    // invalidate in db
                    quotaStore.invalidateTilePages(
                            layerName, invalidateBbox, obj.epsgId, obj.scaleLevel);

                    break; // success, let it go
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
                        log.info(
                                "Aborting seed thread "
                                        + getThreadName()
                                        + ". Error count reached configured maximum of "
                                        + totalFailuresBeforeAborting);
                        super.state = GWCTask.STATE.DEAD;
                        return;
                    }
                    String logMsg =
                            "Invalidate failed at "
                                    + obj
                                    + " after "
                                    + (fetchAttempt + 1)
                                    + " of "
                                    + (tileFailureRetryCount + 1)
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
                                logMsg
                                        + " Skipping and continuing with next tile. Original error: "
                                        + e.getMessage());
                    }
                }
            }

            if (log.isTraceEnabled()) {
                log.trace(getThreadName() + " invalidated " + obj);
            }

            // TilePages i starts with 0
            final long tilesCompletedByThisThread = i + 1;

            updateStatusInfo(tl, tilesCompletedByThisThread, START_TIME);

            checkInterrupted();
        }

        if (!this.terminate) {
            checkInterrupted();

            // remove tiles from file system
            List<TilePage> pages = quotaStore.getInvalidatedTilePages(layerName, false, null);

            List<InvalidateTileRange> tileRanges =
                    pages.stream().map(this::initFromTilePage).collect(Collectors.toList());

            long tileCount = tileRanges.stream().mapToLong(tr -> tr.tileCount()).sum();

            // reset counting
            super.tilesDone.set(0);
            super.tilesTotal = tileCount;

            log.info(
                    "Invalidated pages, will delete tiles: pages="
                            + pages.size()
                            + " possibleTiles="
                            + tileCount);

            // run job in thread pool
            ExecutorService exec = Executors.newFixedThreadPool(poolSize);
            try {
                List<Future<?>> futures = new ArrayList<Future<?>>(tileRanges.size());
                for (InvalidateTileRange tr : tileRanges) {
                    futures.add(exec.submit(new Processor(tr, START_TIME)));
                }
                for (Future<?> f : futures) {
                    try {
                        f.get(); // wait for a processor to complete
                    } catch (InterruptedException | ExecutionException e) {
                        log.warn("Failed execution", e);
                    }
                }
            } finally {
                exec.shutdown();
            }

            // push list to db
            commit();

            // Set correct actual tiles deleted
            this.tilesTotal = this.sharedDeletedCounter.get();
            this.tilesDone.set(this.tilesTotal);

            if (!this.terminate) {
                checkInterrupted();
                // Mark as deleted
                // quotaStore.setDeletedInvalidatedTilePages(layerName);
                // remove invalidate mark
                quotaStore.validateTilePages(layerName, null);
            }
        }

        if (this.terminate) {
            log.info(
                    "Job on "
                            + getThreadName()
                            + " was terminated after "
                            + this.tilesDone
                            + " tiles");
        } else {
            log.info(
                    getThreadName()
                            + " completed "
                            + parsedType.toString()
                            + " layer "
                            + layerName
                            + " after "
                            + this.tilesDone
                            + " tiles and "
                            + this.timeSpent
                            + " seconds.");
        }

        checkInterrupted();

        super.state = GWCTask.STATE.DONE;
    }

    private void reprioritize() {
        Thread.currentThread()
                .setPriority((java.lang.Thread.NORM_PRIORITY + java.lang.Thread.MIN_PRIORITY) / 2);
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
        this.tilesDone.set(this.tilesDone.get() + tilesCount);

        // estimated time of completion in seconds, use a moving average over the last
        this.timeSpent = (int) (System.currentTimeMillis() - start_time) / 1000;

        int threadCount = sharedThreadCount.get();
        long timeTotal =
                Math.round(
                        (double) timeSpent
                                * (((double) tilesTotal / threadCount)
                                        / (double) this.tilesDone.get()));

        this.timeRemaining = (int) (timeTotal - timeSpent);
    }

    public void setFailurePolicy(
            int tileFailureRetryCount,
            long tileFailureRetryWaitTime,
            long totalFailuresBeforeAborting,
            AtomicLong sharedFailureCounter) {
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
    private InvalidateTileRange initFromTilePage(TilePage page) {
        String[] splitted = page.getTileSetId().split("#");
        if (splitted.length < 3 || splitted.length > 4) {
            throw new IllegalArgumentException(
                    "Invalid key for standard tile page, "
                            + "it should have the layer#gridset#format[#paramId]");
        }
        String gridsetId = splitted[1];
        long[] pageCoverage = page.getPageCoverage();
        byte z = page.getZoomLevel();
        long[][] rangeBounds = new long[][] {pageCoverage};

        Map<String, String> parameters = null;
        String parametersKvp = page.getParametersKvp();
        if (parametersKvp != null && !parametersKvp.isEmpty()) {
            parameters =
                    Pattern.compile("&")
                            .splitAsStream(parametersKvp.replace("?", ""))
                            .map(p -> p.split("="))
                            .collect(Collectors.toMap(s -> s[0], s -> s.length > 1 ? s[1] : ""));
        }

        try {
            return new InvalidateTileRange(
                    page,
                    splitted[0],
                    gridsetId,
                    z,
                    z,
                    rangeBounds,
                    MimeType.createFromFormat(splitted[2]),
                    parameters);
        } catch (MimeException e) {
            return null;
        }
    }

    private class DeleteStoreListener implements BlobStoreListener {

        private final InvalidateTileRange tr;

        DeleteStoreListener(InvalidateTileRange tr) {
            this.tr = tr;
        }

        @Override
        public void tileStored(
                String layerName,
                String gridSetId,
                String blobFormat,
                String parametersId,
                long x,
                long y,
                int z,
                long blobSize,
                int epsgId,
                double[] bbox,
                String parametersKvp) {}

        @Override
        public void tileDeleted(
                String layerName,
                String gridSetId,
                String blobFormat,
                String parametersId,
                long x,
                long y,
                int z,
                long blobSize) {
            // Checks that the tile is for this tile range (Could be other jobs deleting tiles)
            if (tr == null || !tr.contains(x, y, z)) {
                return;
            }

            // increment how many tiles are deleted
            sharedDeletedCounter.incrementAndGet();

            // check if it is expired
            if (controlTTL(tr.tilePage)) {
                // Add to list to regenerate
                DeletedTile dt =
                        new DeletedTile(
                                layerName, gridSetId, x, y, z, blobFormat, tr.getParameters());

                if (log.isDebugEnabled()) {
                    log.info("Deleted tile: " + dt.toString());
                }

                if (!deletedTiles.offer(dt)) {
                    // push to db if no space left
                    commit();
                    deletedTiles.offer(dt);
                }
            }
        }

        @Override
        public void tileUpdated(
                String layerName,
                String gridSetId,
                String blobFormat,
                String parametersId,
                long x,
                long y,
                int z,
                long blobSize,
                long oldSize) {}

        @Override
        public void layerDeleted(String layerName) {}

        @Override
        public void layerRenamed(String oldLayerName, String newLayerName) {}

        @Override
        public void gridSubsetDeleted(String layerName, String gridSetId) {}

        @Override
        public void parametersDeleted(String layerName, String parametersId) {}
    }

    /** Pushes elements {@link InvalidateTask#deletedTiles} to database */
    private synchronized void commit() {
        List<DeletedTile> list = new ArrayList<DeletedTile>();

        deletedTiles.drainTo(list);

        try {
            if (list.size() > 0) {
                log.info("Pushing to db: count=" + list.size());
                loadByCopy(list);
            }
        } catch (SQLException | IOException | InterruptedException e) {
            log.warn("Failed to create deleted list", e);
        }
    }

    private void loadByCopy(final List<DeletedTile> list)
            throws SQLException, IOException, InterruptedException {
        final Connection con = dataSource.getConnection();

        try {
            final PGConnection pgConn = getPGConnection(con);

            // InputStreams for copy
            final PipedInputStream copyInputStream = new PipedInputStream();
            try {
                // output stream
                final OutputStream resourceOutputStream = new PipedOutputStream(copyInputStream);

                Thread resourcesThread = null;
                try {
                    resourcesThread = getCopyLoadThread(copyInputStream, pgConn);

                    resourcesThread.start();

                    loadServiceStats(resourceOutputStream, list);
                } finally {
                    IOUtils.closeQuietly(resourceOutputStream);

                    if (resourcesThread != null) {
                        resourcesThread.join();
                    }
                }
            } finally {
                IOUtils.closeQuietly(copyInputStream);
            }
        } finally {
            con.close();
        }
    }

    private Thread getCopyLoadThread(final PipedInputStream in, final PGConnection pgConn) {
        Thread resourcesThread;
        resourcesThread =
                new Thread(
                        new Runnable() {

                            @Override
                            public void run() {
                                try {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Inits COPY service statistics");
                                    }

                                    // copy manager
                                    final CopyManager manager = pgConn.getCopyAPI();

                                    manager.copyIn(getCopyQuery(), in);

                                    if (log.isDebugEnabled()) {
                                        log.debug("COPY in service statistics done");
                                    }
                                } catch (Exception e) {
                                    if (log.isWarnEnabled()) {
                                        log.warn("Failed loading service statistics", e);
                                    }
                                }
                            }
                        });

        return resourcesThread;
    }

    private void loadServiceStats(final OutputStream out, final List<DeletedTile> list)
            throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("Begin");
        }
        try {
            final String encoding = "UTF-8";
            final byte[] separator = "|".getBytes(encoding);
            final byte[] eol = "\n".getBytes(encoding);

            long count = 0;
            for (DeletedTile tile : list) {

                if (count++ % 100 == 0 && log.isDebugEnabled()) {
                    log.debug(String.format("Info: count=%d listSize=%d", count, list.size()));
                }

                try {
                    // group_id, layer_name, gridset_id, x, y, z, mime_type, parameters_kvp

                    // output to copy manager
                    out.write((jobId).getBytes(encoding));
                    out.write(separator);
                    out.write(tile.layerName.getBytes(encoding));
                    out.write(separator);
                    out.write(tile.gridSetId.getBytes(encoding));
                    out.write(separator);
                    out.write(String.valueOf(tile.x).getBytes(encoding));
                    out.write(separator);
                    out.write(String.valueOf(tile.y).getBytes(encoding));
                    out.write(separator);
                    out.write(String.valueOf(tile.z).getBytes(encoding));
                    out.write(separator);
                    out.write(tile.mimeType.getBytes(encoding));
                    out.write(separator);
                    out.write(ParametersUtils.getKvp(tile.parameters).getBytes(encoding));

                    out.write(eol);
                } catch (UnsupportedEncodingException e) {
                    if (log.isWarnEnabled()) {
                        log.warn(
                                String.format(
                                        "Could not convert to %s: '%s'", encoding, e.getMessage()));
                    }
                }
            }
        } finally {
            if (log.isTraceEnabled()) {
                log.trace("End");
            }
        }
    }

    private String getCopyQuery() {
        return String.format(
                "COPY %s (group_id, layer_name, gridset_id, x, y, z, mime_type, parameters_kvp) FROM STDIN WITH DELIMITER '|' NULL '' ENCODING 'UTF8';",
                tableName);
    }

    private PGConnection getPGConnection(Connection conFromPool) throws SQLException {
        try {
            return conFromPool.unwrap(PGConnection.class);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
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

    private class InvalidateTileRange extends TileRange {

        private final TilePage tilePage;

        public InvalidateTileRange(
                TilePage tilePage,
                String layerName,
                String gridSetId,
                int zoomStart,
                int zoomStop,
                long[][] rangeBounds,
                MimeType mimeType,
                Map<String, String> parameters) {
            super(layerName, gridSetId, zoomStart, zoomStop, rangeBounds, mimeType, parameters);

            this.tilePage = tilePage;
        }
    }

    private boolean checkTerminate() {
        return this.terminate;
    }

    private class Processor implements Runnable {

        private final long startTime;
        private final InvalidateTileRange tr;

        public Processor(InvalidateTileRange tr, long startTime) {
            super();
            this.tr = tr;
            this.startTime = startTime;
        }

        @Override
        public void run() {
            try {
                if (!checkTerminate()) {
                    checkInterrupted();

                    long count = tr.tileCount();
                    if (log.isDebugEnabled()) {
                        // Will delete by range
                        log.info("TileRange: tiles=" + count + " " + tr);
                    }
                    // set current tile range
                    DeleteStoreListener listener = new DeleteStoreListener(tr);
                    storageBroker.addBlobStoreListener(listener);
                    storageBroker.delete(tr);
                    storageBroker.removeBlobStoreListener(listener);
                    updateStatusInfo(tl, count, startTime);
                }
            } catch (Exception e) {
                log.warn("Failed deleting", e);
            }
        }
    }
}

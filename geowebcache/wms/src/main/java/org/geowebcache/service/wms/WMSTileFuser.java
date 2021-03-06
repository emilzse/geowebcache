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
 * @author Arne Kepp, OpenGeo, Copyright 2009
 */
package org.geowebcache.service.wms;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import javax.media.jai.PlanarImage;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geotools.resources.image.ImageUtilities;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.conveyor.Conveyor.CacheResult;
import org.geowebcache.conveyor.ConveyorTile;
import org.geowebcache.filter.request.RequestFilterException;
import org.geowebcache.filter.security.SecurityDispatcher;
import org.geowebcache.grid.BoundingBox;
import org.geowebcache.grid.GridSubset;
import org.geowebcache.grid.OutsideCoverageException;
import org.geowebcache.grid.SRS;
import org.geowebcache.io.ImageDecoderContainer;
import org.geowebcache.io.ImageEncoderContainer;
import org.geowebcache.io.Resource;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.layer.wms.WMSLayer;
import org.geowebcache.mime.ImageMime;
import org.geowebcache.mime.MimeType;
import org.geowebcache.stats.RuntimeStats;
import org.geowebcache.storage.StorageBroker;
import org.geowebcache.util.AccountingOutputStream;
import org.geowebcache.util.ServletUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

/*
 * It will work as follows
 * 2) Based on the dimensions and bounding box of the request, GWC will determine the smallest available resolution that equals or exceeds the requested resolution.
 * 3) GWC will create a new in-memory raster, based on the best resolution and requested bounding box, and write the appropriate PNG tiles to it. Missing tiles will be requested from WMS.
 * 4) GWC will scale the raster down to the requested dimensions.
 * 5) GWC will then compress the raster to the desired output format and return the image. The image is not cached.
 */
public class WMSTileFuser {
    private static Log log = LogFactory.getLog(WMSTileFuser.class);

    /**
     * Available to parse GTWMSLayer with special deserializer
     *
     * @see WMSLayerDeserializer
     */
    private static final Gson GSON;

    private static final ExecutorService EXECUTOR_SERVICE;

    // Creating shared object
    private static final BlockingQueue<TilesInfo> SHARED_QUEUE;

    static {
        GSON = new GsonBuilder().create();
        // pool of 20 threads checking/seeding missing tiles
        EXECUTOR_SERVICE = Executors.newFixedThreadPool(20);
        SHARED_QUEUE = new LinkedBlockingQueue<TilesInfo>();

        Thread consThread = new Thread(new TilesConsumer(SHARED_QUEUE));

        consThread.start();
    }

    private ApplicationContext applicationContext;

    final StorageBroker sb;

    final GridSubset gridSubset;

    final TileLayer layer;

    final ImageMime outputFormat;

    ImageMime srcFormat;

    int reqHeight;

    int reqWidth;

    // GetTiles
    boolean seeding;

    // GetTiles
    int maxMiss;

    // The desired extent of the request
    final BoundingBox reqBounds;

    // Boolean reqTransparent;

    // String reqBgColor;

    // For adjustment of final raster
    double xResolution;

    double yResolution;

    // The source resolution
    /* Follows GIS rather than Graphics conventions and so is expressed as physical size of pixel
     * rather than density.*/
    double srcResolution;

    int srcIdx;

    // Area of tiles being used in tile coordinates
    long[] srcRectangle;

    // The spatial extent of the tiles used to fulfil the request
    BoundingBox srcBounds;
    //
    BoundingBox canvasBounds;
    /** Canvas dimensions */
    int[] canvasSize = new int[2];

    static class SpatialOffsets {
        double top;
        double bottom;
        double left;
        double right;
    };

    static class PixelOffsets {
        int top;
        int bottom;
        int left;
        int right;
    };

    /** These are values before scaling */
    PixelOffsets canvOfs = new PixelOffsets();

    SpatialOffsets boundOfs = new SpatialOffsets();
    /** Mosaic image */
    BufferedImage canvas;
    /** Graphics object used for drawing the tiles into a mosaic */
    Graphics2D gfx;

    /** Layer parameters */
    private Map<String, String> fullParameters;

    /** Map of all the possible decoders to use */
    private ImageDecoderContainer decoderMap;

    /** Map of all the possible encoders to use */
    private ImageEncoderContainer encoderMap;

    /** Hints used for writing the BufferedImage on the canvas */
    private RenderingHints hints;

    private SecurityDispatcher securityDispatcher;

    /** Enum storing the Hints associated to one of the 3 configurations(SPEED, QUALITY, DEFAULT) */
    public enum HintsLevel {
        QUALITY(0, "quality"),
        DEFAULT(1, "default"),
        SPEED(2, "speed");

        private RenderingHints hints;

        private String mode;

        HintsLevel(int numHint, String mode) {
            this.mode = mode;
            switch (numHint) {
                    // QUALITY HINTS
                case 0:
                    hints =
                            new RenderingHints(
                                    RenderingHints.KEY_COLOR_RENDERING,
                                    RenderingHints.VALUE_COLOR_RENDER_QUALITY);
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_ANTIALIASING,
                                    RenderingHints.VALUE_ANTIALIAS_ON));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_FRACTIONALMETRICS,
                                    RenderingHints.VALUE_FRACTIONALMETRICS_ON));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_ALPHA_INTERPOLATION,
                                    RenderingHints.VALUE_ALPHA_INTERPOLATION_QUALITY));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_INTERPOLATION,
                                    RenderingHints.VALUE_INTERPOLATION_BICUBIC));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_RENDERING,
                                    RenderingHints.VALUE_RENDER_QUALITY));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_TEXT_ANTIALIASING,
                                    RenderingHints.VALUE_TEXT_ANTIALIAS_ON));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_STROKE_CONTROL,
                                    RenderingHints.VALUE_STROKE_NORMALIZE));
                    break;
                    // DEFAULT HINTS
                case 1:
                    hints =
                            new RenderingHints(
                                    RenderingHints.KEY_COLOR_RENDERING,
                                    RenderingHints.VALUE_COLOR_RENDER_DEFAULT);
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_ANTIALIASING,
                                    RenderingHints.VALUE_ANTIALIAS_DEFAULT));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_FRACTIONALMETRICS,
                                    RenderingHints.VALUE_FRACTIONALMETRICS_DEFAULT));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_ALPHA_INTERPOLATION,
                                    RenderingHints.VALUE_ALPHA_INTERPOLATION_DEFAULT));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_INTERPOLATION,
                                    RenderingHints.VALUE_INTERPOLATION_BILINEAR));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_RENDERING,
                                    RenderingHints.VALUE_RENDER_DEFAULT));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_TEXT_ANTIALIASING,
                                    RenderingHints.VALUE_TEXT_ANTIALIAS_DEFAULT));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_STROKE_CONTROL,
                                    RenderingHints.VALUE_STROKE_DEFAULT));
                    break;
                    // SPEED HINTS
                case 2:
                    hints =
                            new RenderingHints(
                                    RenderingHints.KEY_COLOR_RENDERING,
                                    RenderingHints.VALUE_COLOR_RENDER_SPEED);
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_ANTIALIASING,
                                    RenderingHints.VALUE_ANTIALIAS_OFF));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_FRACTIONALMETRICS,
                                    RenderingHints.VALUE_FRACTIONALMETRICS_OFF));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_ALPHA_INTERPOLATION,
                                    RenderingHints.VALUE_ALPHA_INTERPOLATION_SPEED));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_INTERPOLATION,
                                    RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_RENDERING,
                                    RenderingHints.VALUE_RENDER_SPEED));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_TEXT_ANTIALIASING,
                                    RenderingHints.VALUE_TEXT_ANTIALIAS_OFF));
                    hints.add(
                            new RenderingHints(
                                    RenderingHints.KEY_STROKE_CONTROL,
                                    RenderingHints.VALUE_STROKE_PURE));
                    break;
            }
        }

        public RenderingHints getRenderingHints() {
            return hints;
        }

        public String getModeName() {
            return mode;
        }

        public static HintsLevel getHintsForMode(String mode) {

            if (mode != null) {
                if (mode.equalsIgnoreCase(QUALITY.getModeName())) {
                    return QUALITY;
                } else if (mode.equalsIgnoreCase(SPEED.getModeName())) {
                    return SPEED;
                } else {
                    return DEFAULT;
                }
            } else {
                return DEFAULT;
            }
        }
    }

    protected WMSTileFuser(TileLayerDispatcher tld, StorageBroker sb, HttpServletRequest servReq)
            throws GeoWebCacheException {
        this(tld, sb, servReq, null);
    }

    protected WMSTileFuser(
            TileLayerDispatcher tld, StorageBroker sb, HttpServletRequest servReq, String gridSetId)
            throws GeoWebCacheException {
        this.sb = sb;

        String[] keys = {
            "layers",
            "format",
            "srs",
            "bbox",
            "width",
            "height",
            "transparent",
            "bgcolor",
            "hints",
            "max_miss",
            "seeding"
        };

        Map<String, String> values =
                ServletUtils.selectedStringsFromMap(
                        servReq.getParameterMap(), servReq.getCharacterEncoding(), keys);

        // TODO Parameter filters?

        String layerName = values.get("layers");
        layer = tld.getTileLayer(layerName);

        // use selected gridset
        gridSubset =
                gridSetId == null
                        ? layer.getGridSubsetForSRS(SRS.getSRS(values.get("srs")))
                        : layer.getGridSubset(gridSetId);

        outputFormat = (ImageMime) ImageMime.createFromFormat(values.get("format"));

        List<MimeType> ml = layer.getMimeTypes();
        Iterator<MimeType> iter = ml.iterator();

        ImageMime firstMt = null;

        if (iter.hasNext()) {
            firstMt = (ImageMime) iter.next();
        }

        boolean outputJpeg = outputFormat.getInternalName().equalsIgnoreCase("jpeg");
        while (iter.hasNext()) {
            MimeType mt = iter.next();

            // Sets first here so that it will be part of loop
            if (firstMt == null) {
                firstMt = (ImageMime) mt;
            }

            if (outputJpeg) {
                // use png for stitching to jpeg
                if (mt.getFileExtension().equalsIgnoreCase("png")) {
                    this.srcFormat = (ImageMime) mt;
                    break;
                }
            }

            // if supported format matches requested format use it
            if (mt.getMimeType().equalsIgnoreCase(outputFormat.getMimeType())
                    && mt.getFileExtension().equalsIgnoreCase(outputFormat.getFileExtension())) {
                this.srcFormat = (ImageMime) mt;
            }
        }

        if (srcFormat == null) {
            srcFormat = firstMt;
        }

        reqBounds = new BoundingBox(values.get("bbox"));

        reqWidth = Integer.valueOf(values.get("width"));

        reqHeight = Integer.valueOf(values.get("height"));

        // GetTiles
        // if seeding should be done for missing tiles
        seeding = BooleanUtils.toBoolean(values.get("seeding"));
        // how many misses before stopping checking
        maxMiss = values.get("max_miss") == null ? -1 : Integer.valueOf(values.get("max_miss"));

        fullParameters =
                layer.getModifiableParameters(
                        servReq.getParameterMap(), servReq.getCharacterEncoding());
        if (values.get("hints") != null) {
            hints = HintsLevel.getHintsForMode(values.get("hints")).getRenderingHints();
        }
    }

    /**
     * This was used for unit tests and should not have been used elsewhere. It will likely cause
     * NullPointerExceptions if used in production. Use WMSTileFuser(TileLayerDispatcher tld,
     * StorageBroker sb, HttpServletRequest servReq) instead. It will be removed in future.
     */
    @Deprecated
    protected WMSTileFuser(
            TileLayer layer, GridSubset gridSubset, BoundingBox bounds, int width, int height) {
        this.sb = null;
        this.outputFormat = ImageMime.png;
        this.layer = layer;
        this.gridSubset = gridSubset;
        this.reqBounds = bounds;
        this.reqWidth = width;
        this.reqHeight = height;
        this.fullParameters = Collections.emptyMap();

        List<MimeType> ml = layer.getMimeTypes();
        Iterator<MimeType> iter = ml.iterator();
        ImageMime firstMt = null;

        if (iter.hasNext()) {
            firstMt = (ImageMime) iter.next();
        }

        while (iter.hasNext()) {
            MimeType mt = iter.next();
            if (mt.getInternalName().equalsIgnoreCase("png")) {
                this.srcFormat = (ImageMime) mt;
                break;
            }
        }

        if (srcFormat == null) {
            srcFormat = firstMt;
        }
    }

    protected void determineSourceResolution() {
        xResolution = reqBounds.getWidth() / reqWidth;
        yResolution = reqBounds.getHeight() / reqHeight;

        double tmpResolution;
        // We use the smallest one
        if (yResolution < xResolution) {
            tmpResolution = yResolution;
        } else {
            tmpResolution = xResolution;
        }

        log.debug(
                "x res: "
                        + xResolution
                        + " y res: "
                        + yResolution
                        + " tmpResolution: "
                        + tmpResolution);

        // Cut ourselves 0.5% slack
        double compResolution = 1.005 * tmpResolution;

        double[] resArray = gridSubset.getResolutions();

        for (srcIdx = 0; srcIdx < resArray.length; srcIdx++) {
            srcResolution = resArray[srcIdx];
            if (srcResolution < compResolution) {
                break;
            }
        }

        if (srcIdx >= resArray.length) {
            srcIdx = resArray.length - 1;
        }

        log.debug("z: " + srcIdx + " , resolution: " + srcResolution + " (" + tmpResolution + ")");

        // At worst, we have the best resolution possible
    }

    protected void determineCanvasLayout() throws OutsideCoverageException {
        // Find the spatial extent of the tiles needed to cover the desired extent
        srcRectangle = gridSubset.getCoverageIntersection(srcIdx, reqBounds);
        srcBounds = gridSubset.boundsFromRectangle(srcRectangle);

        // Will control that req intersects src at all to avoid #497
        if (!reqBounds.intersects(srcBounds)) {
            String msg =
                    String.format(
                            "Request BBOX do not intersect with source: req=%s src=%s",
                            reqBounds.toString(), srcBounds.toString());
            log.debug(msg);

            throw new OutsideCoverageException(msg);
        }

        // We now have the complete area, lets figure out our offsets
        // Positive means that there is blank space to the first tile,
        // negative means we will not use the entire tile
        boundOfs.left = srcBounds.getMinX() - reqBounds.getMinX();
        boundOfs.bottom = srcBounds.getMinY() - reqBounds.getMinY();
        boundOfs.right = reqBounds.getMaxX() - srcBounds.getMaxX();
        boundOfs.top = reqBounds.getMaxY() - srcBounds.getMaxY();

        canvasSize[0] = (int) Math.round(reqBounds.getWidth() / this.srcResolution);
        canvasSize[1] = (int) Math.round(reqBounds.getHeight() / this.srcResolution);

        PixelOffsets naiveOfs = new PixelOffsets();
        // Calculate the corresponding pixel offsets. We'll stick to sane,
        // i.e. bottom left, coordinates at this point
        naiveOfs.left = (int) Math.round(boundOfs.left / this.srcResolution);
        naiveOfs.bottom = (int) Math.round(boundOfs.bottom / this.srcResolution);
        naiveOfs.right = (int) Math.round(boundOfs.right / this.srcResolution);
        naiveOfs.top = (int) Math.round(boundOfs.top / this.srcResolution);

        // Find the offsets on the opposite sides.  This is dependent of how the first two were
        // rounded.

        // First, find a tile boundary near the canvas edge, then make sure it's on the correct
        // side to match the corresponding boundOfs, then take the modulo of the naive rounding
        // based on the boundOfs, then subtract the two and apply the difference to the boundOfs.
        int tileWidth = this.gridSubset.getTileWidth();
        int tileHeight = this.gridSubset.getTileHeight();

        canvOfs.left = naiveOfs.left;
        canvOfs.bottom = naiveOfs.bottom;

        canvOfs.right = (canvasSize[0] - canvOfs.left) % tileWidth; // Find nearby tile boundary
        canvOfs.right =
                (Integer.signum(naiveOfs.right) * tileWidth + canvOfs.right)
                        % tileWidth; // Ensure same sign as naive calculation
        canvOfs.right =
                canvOfs.right
                        - (naiveOfs.right % tileWidth)
                        + naiveOfs.right; // Find adjustment from naive and apply to naive
        // calculation
        canvOfs.top = (canvasSize[1] - canvOfs.bottom) % tileHeight; // Find nearby tile boundary
        canvOfs.top =
                (Integer.signum(naiveOfs.top) * tileHeight + canvOfs.top)
                        % tileHeight; // Ensure same sign as naive calculation
        canvOfs.top =
                canvOfs.top
                        - (naiveOfs.top % tileHeight)
                        + naiveOfs.top; // Find adjustment from naive and apply to naive calculation

        // postconditions
        assert Math.abs(canvOfs.left - naiveOfs.left) <= 1;
        assert Math.abs(canvOfs.bottom - naiveOfs.bottom) <= 1;
        assert Math.abs(canvOfs.right - naiveOfs.right) <= 1;
        assert Math.abs(canvOfs.top - naiveOfs.top) <= 1;

        if (log.isDebugEnabled()) {
            log.debug("intersection rectangle: " + Arrays.toString(srcRectangle));
            log.debug("intersection bounds: " + srcBounds + " (" + reqBounds + ")");
            log.debug(
                    "Bound offsets: "
                            + Arrays.toString(
                                    new double[] {
                                        boundOfs.left, boundOfs.bottom, boundOfs.right, boundOfs.top
                                    }));
            log.debug(
                    "Canvas size: "
                            + Arrays.toString(canvasSize)
                            + "("
                            + reqWidth
                            + ","
                            + reqHeight
                            + ")");
            log.debug(
                    "Canvas offsets: "
                            + Arrays.toString(
                                    new int[] {
                                        canvOfs.left, canvOfs.bottom, canvOfs.right, canvOfs.top
                                    }));
        }
    }

    protected void createCanvas() {
        // TODO take bgcolor and transparency from request into account
        // should move this into a separate function

        Color bgColor = null;
        boolean transparent = true;

        if (layer instanceof WMSLayer) {
            WMSLayer wmsLayer = (WMSLayer) layer;
            int[] colorAr = wmsLayer.getBackgroundColor();

            if (colorAr != null) {
                bgColor = new Color(colorAr[0], colorAr[1], colorAr[2]);
            }
            transparent = wmsLayer.getTransparent();
        }

        int canvasType;
        if (bgColor == null
                && transparent
                && (outputFormat.supportsAlphaBit() || outputFormat.supportsAlphaChannel())) {
            canvasType = BufferedImage.TYPE_INT_ARGB;
        } else {
            canvasType = BufferedImage.TYPE_INT_RGB;
            if (bgColor == null) {
                bgColor = Color.WHITE;
            }
        }

        // Create the actual canvas and graphics object
        canvas = new BufferedImage(canvasSize[0], canvasSize[1], canvasType);
        gfx = (Graphics2D) canvas.getGraphics();

        if (bgColor != null) {
            gfx.setColor(bgColor);
            gfx.fillRect(0, 0, canvasSize[0], canvasSize[1]);
        }

        // Hints settings
        RenderingHints hintsTemp = HintsLevel.DEFAULT.getRenderingHints();

        if (hints != null) {
            hintsTemp = hints;
        }
        gfx.addRenderingHints(hintsTemp);
    }

    protected void renderCanvas()
            throws OutsideCoverageException, GeoWebCacheException, IOException, Exception {

        // Now we loop over all the relevant tiles and write them to the canvas,
        // Starting at the bottom, moving to the right and up

        // Bottom row of tiles, in tile coordinates
        long starty = srcRectangle[1];

        // gridy is the tile row index
        for (long gridy = starty; gridy <= srcRectangle[3]; gridy++) {

            int tiley = 0;
            int canvasy = (int) (srcRectangle[3] - gridy) * gridSubset.getTileHeight();
            int tileHeight = gridSubset.getTileHeight();

            if (canvOfs.top > 0) {
                // Add padding
                canvasy += canvOfs.top;
            } else {
                // Top tile is cut off
                if (gridy == srcRectangle[3]) {
                    // This one starts at the top, so canvasy remains 0
                    tileHeight = tileHeight + canvOfs.top;
                    tiley = -canvOfs.top;
                } else {
                    // Offset that the first tile contributed,
                    // rather, we subtract what it did not contribute
                    canvasy += canvOfs.top;
                }
            }

            if (gridy == srcRectangle[1] && canvOfs.bottom < 0) {
                // Check whether we only use part of the first tiles (bottom row)
                // Offset is negative, slice the bottom off the tile
                tileHeight += canvOfs.bottom;
            }

            long startx = srcRectangle[0];
            for (long gridx = startx; gridx <= srcRectangle[2]; gridx++) {

                long[] gridLoc = {gridx, gridy, srcIdx};

                ConveyorTile tile =
                        new ConveyorTile(
                                sb,
                                layer.getName(),
                                gridSubset.getName(),
                                gridLoc,
                                srcFormat,
                                fullParameters,
                                null,
                                null);

                // set current layer
                tile.setTileLayer(layer);

                securityDispatcher.checkSecurity(tile);

                // Check whether this tile is to be rendered at all
                try {
                    layer.applyRequestFilters(tile);
                } catch (RequestFilterException e) {
                    log.debug(e.getMessage(), e);
                    continue;
                }

                // Will try until gets a proper response
                ConveyorTile singleTile = null;
                for (int i = 0; singleTile == null && i < 100; i++) {
                    try {
                        singleTile = layer.getTile(tile);

                        if (singleTile == null) {
                            log.warn(
                                    "Failed getting tile, will retry: count="
                                            + i
                                            + " layer="
                                            + layer.getName());
                        }
                    } catch (Exception e) {
                        log.warn(
                                "Failed getting tile, will retry: count="
                                        + i
                                        + " layer="
                                        + layer.getName()
                                        + " msg="
                                        + (e.getMessage() == null && e.getCause() != null
                                                ? e.getCause().getMessage()
                                                : e.getMessage()));

                        if (log.isDebugEnabled()) {
                            log.debug("Failed getting tile, will retry", e);
                        }
                    }
                }

                if (singleTile == null) {
                    log.error(
                            "Was not able to get tile during tile stitching: layer="
                                    + layer.getName());

                    continue;
                }

                // Selection of the resource input stream
                Resource blob = tile.getBlob();
                // Extraction of the image associated with the defined MimeType
                String formatName = srcFormat.getMimeType();
                BufferedImage tileImg =
                        decoderMap.decode(
                                formatName,
                                blob,
                                decoderMap.isAggressiveInputStreamSupported(formatName),
                                null);

                int tilex = 0;
                int canvasx = (int) (gridx - startx) * gridSubset.getTileWidth();
                int tileWidth = gridSubset.getTileWidth();

                if (canvOfs.left > 0) {
                    // Add padding
                    canvasx += canvOfs.left;
                } else {
                    // Leftmost tile is cut off
                    if (gridx == srcRectangle[0]) {
                        // This one starts to the left top, so canvasx remains 0
                        tileWidth = tileWidth + canvOfs.left;
                        tilex = -canvOfs.left;
                    } else {
                        // Offset that the first tile contributed,
                        // rather, we subtract what it did not contribute
                        canvasx += canvOfs.left;
                    }
                }

                if (gridx == srcRectangle[2] && canvOfs.right < 0) {
                    // Check whether we only use part of the first tiles (bottom row)
                    // Offset is negative, slice the bottom off the tile
                    tileWidth = tileWidth + canvOfs.right;
                }

                // TODO We should really ensure we can never get here
                if (tileWidth == 0 || tileHeight == 0) {
                    log.debug("tileWidth: " + tileWidth + " tileHeight: " + tileHeight);
                    continue;
                }

                // Cut down the tile to the part we want
                if (tileWidth != gridSubset.getTileWidth()
                        || tileHeight != gridSubset.getTileHeight()) {
                    log.debug(
                            "tileImg.getSubimage("
                                    + tilex
                                    + ","
                                    + tiley
                                    + ","
                                    + tileWidth
                                    + ","
                                    + tileHeight
                                    + ")");
                    tileImg = tileImg.getSubimage(tilex, tiley, tileWidth, tileHeight);
                }

                // Render the tile on the big canvas
                log.debug(
                        "drawImage(subtile,"
                                + canvasx
                                + ","
                                + canvasy
                                + ",null) "
                                + Arrays.toString(gridLoc));

                gfx.drawImage(tileImg, canvasx, canvasy, null); // imageObserver
            }
        }

        gfx.dispose();
    }

    /**
     * @return information about missing tiles
     * @throws OutsideCoverageException
     * @throws GeoWebCacheException
     * @throws IOException
     * @throws Exception
     */
    private Map<String, Integer> getMissingTiles()
            throws OutsideCoverageException, GeoWebCacheException, IOException, Exception {
        List<Future<?>> futures = new ArrayList<Future<?>>();

        AtomicInteger hit = new AtomicInteger(0);
        AtomicInteger miss = new AtomicInteger(0);

        // Now we loop over all the relevant tiles and create job to check if tile is
        // missing

        // Bottom row of tiles, in tile coordinates
        long starty = srcRectangle[1];

        // gridy is the tile row index
        for (long gridy = starty; gridy <= srcRectangle[3]; gridy++) {
            long startx = srcRectangle[0];
            for (long gridx = startx; gridx <= srcRectangle[2]; gridx++) {
                futures.add(
                        EXECUTOR_SERVICE.submit(
                                new CheckTileExistsTask(gridx, gridy, hit, miss, seeding)));
            }
        }

        for (Future<?> future : futures) {
            future.get(); // check if future is done

            if (maxMiss > 0 && miss.get() >= maxMiss) {
                break;
            }
        }

        // the map with result
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("hit", hit.get());
        map.put("miss", miss.get());
        map.put("total", hit.get() + miss.get());

        return map;
    }

    final class CheckTileExistsTask implements Runnable {

        private final long gridX;

        private final long gridY;

        private final AtomicInteger hit;

        private final AtomicInteger miss;

        private final boolean seeding;

        /**
         * @param gridX tile x position
         * @param gridY tile y position
         * @param hit counter for hit tiles
         * @param miss counter for miss tiles
         * @param seeding if missing tile should be seeded
         */
        public CheckTileExistsTask(
                long gridX, long gridY, AtomicInteger hit, AtomicInteger miss, boolean seeding) {
            this.gridX = gridX;
            this.gridY = gridY;
            this.hit = hit;
            this.miss = miss;
            this.seeding = seeding;
        }

        @Override
        public void run() {
            long[] gridLoc = {gridX, gridY, srcIdx};

            ConveyorTile tile =
                    new ConveyorTile(
                            sb,
                            layer.getName(),
                            gridSubset.getName(),
                            gridLoc,
                            srcFormat,
                            fullParameters,
                            null,
                            null);

            // Check whether this tile is to be rendered at all
            try {
                layer.applyRequestFilters(tile);
            } catch (RequestFilterException e) {
                log.debug(e.getMessage(), e);
                return;
            }

            // Check if tile is generated
            if (layer instanceof WMSLayer) {
                if (((WMSLayer) layer).tryCacheFetch(tile)) {
                    hit.incrementAndGet();
                } else {
                    miss.incrementAndGet();

                    // seed the tile
                    if (seeding) {
                        try {
                            SHARED_QUEUE.put(new TilesInfo((WMSLayer) layer, tile));
                        } catch (InterruptedException e) {
                            log.warn("Interrupted put", e);
                        }
                    }
                }
            }
        }
    }

    protected void scaleRaster() {
        if (canvasSize[0] != reqWidth || canvasSize[1] != reqHeight) {
            BufferedImage preTransform = canvas;

            canvas = new BufferedImage(reqWidth, reqHeight, preTransform.getType());

            Graphics2D gfx = canvas.createGraphics();

            AffineTransform affineTrans =
                    AffineTransform.getScaleInstance(
                            ((double) reqWidth) / preTransform.getWidth(),
                            ((double) reqHeight) / preTransform.getHeight());

            log.debug(
                    "AffineTransform: "
                            + (((double) reqWidth) / preTransform.getWidth())
                            + ","
                            + +(((double) reqHeight) / preTransform.getHeight()));
            // Hints settings
            RenderingHints hintsTemp = HintsLevel.DEFAULT.getRenderingHints();

            if (hints != null) {
                hintsTemp = hints;
            }
            gfx.addRenderingHints(hintsTemp);
            gfx.drawRenderedImage(preTransform, affineTrans);
            gfx.dispose();
        }
    }

    protected void writeResponse(HttpServletResponse response, RuntimeStats stats)
            throws IOException, OutsideCoverageException, GeoWebCacheException, Exception {
        determineSourceResolution();
        determineCanvasLayout();
        createCanvas();
        renderCanvas();
        scaleRaster();

        AccountingOutputStream aos = null;
        RenderedImage finalImage = null;
        try {
            finalImage = canvas;

            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(this.outputFormat.getMimeType());
            response.setCharacterEncoding("UTF-8");

            ServletOutputStream os = response.getOutputStream();
            aos = new AccountingOutputStream(os);

            // Image encoding with the associated writer
            encoderMap.encode(
                    finalImage,
                    outputFormat,
                    aos,
                    encoderMap.isAggressiveOutputStreamSupported(outputFormat.getMimeType()),
                    null);

            log.debug("WMS response size: " + aos.getCount() + "bytes.");
            stats.log(aos.getCount(), CacheResult.WMS);
        } catch (Exception e) {
            log.debug("IOException writing untiled response to client: " + e.getMessage(), e);

            // closing the stream
            if (aos != null) {
                IOUtils.closeQuietly(aos);
            }

            // releasing Image
            if (finalImage != null) {
                ImageUtilities.disposePlanarImageChain(PlanarImage.wrapRenderedImage(finalImage));
            }
        }
    }

    /**
     * Get missing tiles
     *
     * @param response
     * @throws IOException
     * @throws OutsideCoverageException
     * @throws GeoWebCacheException
     * @throws Exception
     */
    protected void writeMissingTilesResponse(HttpServletResponse response)
            throws IOException, OutsideCoverageException, GeoWebCacheException, Exception {
        determineSourceResolution();
        determineCanvasLayout();

        try {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");

            try {
                // returns response with information about hit/miss/total
                IOUtils.write(GSON.toJson(getMissingTiles()), response.getOutputStream());
                response.flushBuffer();
            } catch (IOException ioe) {
                // Do nothing
            }

        } catch (Exception e) {
            log.debug("Could not read missing tiles: " + e.getMessage(), e);
        }
    }

    /**
     * Setting of the ApplicationContext associated for extracting the related beans
     *
     * @param context
     * @throws BeansException
     */
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        applicationContext = context;
        decoderMap = applicationContext.getBean(ImageDecoderContainer.class);
        encoderMap = applicationContext.getBean(ImageEncoderContainer.class);
    }

    /**
     * Setting of the hints configuration taken from the WMSService
     *
     * @param hintsConfig
     */
    public void setHintsConfiguration(String hintsConfig) {
        if (hints == null) {
            hints = HintsLevel.getHintsForMode(hintsConfig).getRenderingHints();
        }
    }

    public void setSecurityDispatcher(SecurityDispatcher securityDispatcher) {
        this.securityDispatcher = securityDispatcher;
    }

    /**
     * Consumer for seeding missing tiles
     *
     * @author ez
     */
    private static class TilesConsumer implements Runnable {

        private final BlockingQueue<TilesInfo> sharedQueue;

        public TilesConsumer(BlockingQueue<TilesInfo> sharedQueue) {
            this.sharedQueue = sharedQueue;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    TilesInfo info = sharedQueue.take();

                    // Will try until gets a proper response
                    ConveyorTile singleTile = null;
                    for (int i = 0; singleTile == null && i < 100; i++) {
                        try {
                            singleTile = info.layer.getTile(info.tile);

                            if (singleTile == null) {
                                log.warn(
                                        "Failed creating tile in consumer, will retry: count="
                                                + i
                                                + " layer="
                                                + info.layer.getName());
                            }
                        } catch (Exception e) {
                            log.warn(
                                    "Failed creating tile in consumer, will retry: count="
                                            + i
                                            + " layer="
                                            + info.layer.getName()
                                            + " msg="
                                            + e.getMessage());
                        }
                    }

                    if (singleTile == null) {
                        log.error(
                                "Was not able to create tile in consumer: layer="
                                        + info.layer.getName());

                        continue;
                    }
                } catch (InterruptedException ex) {
                    log.error("Consumer interrupted", ex);

                    break;
                }
            }
        }
    }

    private class TilesInfo {

        private final WMSLayer layer;
        private final ConveyorTile tile;

        public TilesInfo(WMSLayer layer, ConveyorTile tile) {
            super();
            this.layer = layer;
            this.tile = tile;
        }
    }
}

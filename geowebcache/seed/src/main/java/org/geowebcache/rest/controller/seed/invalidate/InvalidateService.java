package org.geowebcache.rest.controller.seed.invalidate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.config.ConfigurationDispatcher;
import org.geowebcache.config.ConfigurationException;
import org.geowebcache.config.ContextualConfigurationProvider;
import org.geowebcache.diskquota.QuotaStoreProvider;
import org.geowebcache.grid.BoundingBox;
import org.geowebcache.io.GeoWebCacheXStream;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.InvalidateConfig;
import org.geowebcache.seed.InvalidateRequest;
import org.geowebcache.seed.TileBreeder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class InvalidateService extends AbsService {

    private static Log log = LogFactory.getLog(InvalidateService.class);

    @Autowired TileBreeder seeder;

    @Autowired private QuotaStoreProvider quotaStoreProvider;

    @Autowired private ConfigurationDispatcher configDispatcher;

    protected XStream configXStream(XStream xs) {
        return configDispatcher.getConfiguredXStreamWithContext(
                xs, ContextualConfigurationProvider.Context.REST);
    }

    protected ResponseEntity<?> handleRequest(
            String layerName, String formatExtension, String body, String jobId, int poolSize) {
        XStream xs = configXStream(new GeoWebCacheXStream(new DomDriver()));

        InvalidateRequest obj = null;

        if (formatExtension == null || formatExtension.equalsIgnoreCase("xml")) {
            obj = (InvalidateRequest) xs.fromXML(body);
        } else if (formatExtension.equalsIgnoreCase("json")) {
            String text = body;
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                InvalidateConfigRequest[] ic =
                        objectMapper.readValue(text, InvalidateConfigRequest[].class);
                InvalidateConfig[] configs =
                        Arrays.stream(ic)
                                .map(
                                        o ->
                                                new InvalidateConfig(
                                                        new BoundingBox(
                                                                o.bbox[0], o.bbox[1], o.bbox[2],
                                                                o.bbox[3]),
                                                        o.epsgId,
                                                        o.scaleLevel))
                                .toArray(InvalidateConfig[]::new);

                obj = new InvalidateRequest(configs);
            } catch (Exception e) {
                log.info(e.getMessage(), e);

                return new ResponseEntity<Object>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } else {
            return new ResponseEntity<Object>(
                    "Format extension unknown or not specified: " + formatExtension,
                    HttpStatus.BAD_REQUEST);
        }

        return handleRequest(layerName, obj, jobId, poolSize);
    }

    protected ResponseEntity<?> handleRequest(
            String layerName, InvalidateRequest obj, String jobId, int poolSize) {
        try {
            GWCTask[] tasks = createTasks(seeder.findTileLayer(layerName), obj, jobId, poolSize);
            seeder.dispatchTasks(tasks);

            return new ResponseEntity<Object>(
                    Arrays.stream(tasks).mapToLong(GWCTask::getTaskId).toArray(), HttpStatus.OK);
        } catch (GeoWebCacheException e) {
            return new ResponseEntity<Object>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private GWCTask[] createTasks(TileLayer tl, InvalidateRequest ir, String jobId, int poolSize)
            throws GeoWebCacheException {
        InvalidateTask task;
        try {
            task =
                    new InvalidateTask(
                            seeder.getStorageBroker(),
                            quotaStoreProvider.getQuotaStore(),
                            tl,
                            ir.getInvalidateItems(),
                            dataSource,
                            tableName,
                            jobId,
                            seeder.getMaxPoolSize() < poolSize
                                    ? seeder.getMaxPoolSize()
                                    : poolSize);
        } catch (ConfigurationException | IOException e) {
            throw new GeoWebCacheException(e);
        }

        AtomicLong failureCounter = new AtomicLong();
        AtomicInteger sharedThreadCount = new AtomicInteger();

        task.setFailurePolicy(
                seeder.getTileFailureRetryCount(),
                seeder.getTileFailureRetryWaitTime(),
                seeder.getTotalFailuresBeforeAborting(),
                failureCounter);

        task.setThreadInfo(sharedThreadCount, 0);

        return new GWCTask[] {task};
    }

    public void setTileBreeder(TileBreeder seeder) {
        this.seeder = seeder;
    }
}

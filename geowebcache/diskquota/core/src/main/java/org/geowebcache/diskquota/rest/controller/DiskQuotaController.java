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
 * @author David Vick, Boundless, Copyright 2017
 */
package org.geowebcache.diskquota.rest.controller;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.HierarchicalStreamDriver;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import com.thoughtworks.xstream.io.json.JsonHierarchicalStreamDriver;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.geowebcache.config.ConfigurationDispatcher;
import org.geowebcache.config.ContextualConfigurationProvider.Context;
import org.geowebcache.diskquota.ConfigLoader;
import org.geowebcache.diskquota.DiskQuotaConfig;
import org.geowebcache.diskquota.DiskQuotaMonitor;
import org.geowebcache.diskquota.storage.Quota;
import org.geowebcache.io.GeoWebCacheXStream;
import org.geowebcache.storage.blobstore.memory.CacheStatistics;
import org.geowebcache.util.ApplicationContextProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.WebApplicationContext;

@Component
@RestController
@RequestMapping(path = "${gwc.context.suffix:}/rest")
public class DiskQuotaController {

    private final WebApplicationContext context;

    @Autowired
    public DiskQuotaController(ApplicationContextProvider appCtx) {
        context = appCtx == null ? null : appCtx.getApplicationContext();
    }

    static final Log log = LogFactory.getLog(DiskQuotaController.class);

    @Autowired DiskQuotaMonitor monitor;
    @Autowired ConfigurationDispatcher xmlConfig;

    public void setDiskQuotaMonitor(DiskQuotaMonitor monitor) {
        this.monitor = monitor;
    }

    @RequestMapping(value = "/diskquota", method = RequestMethod.GET)
    public ResponseEntity<?> doGet(HttpServletRequest request) {
        final DiskQuotaConfig config = monitor.getConfig();

        if (request.getPathInfo().contains("json")) {
            try {
                return getJsonRepresentation(config);
            } catch (JSONException e) {
                return new ResponseEntity<Object>(
                        "Caught JSON Execption.", HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } else {
            return getXmlRepresentation(config);
        }
    }

    @RequestMapping(value = "/diskquota/{layer}", method = RequestMethod.GET)
    public ResponseEntity<?> doGet(HttpServletRequest request, @PathVariable("layer") String layer) {
        try {
            Quota usedQuota = monitor.getUsedQuotaByLayerName(layer);
            if (!request.getPathInfo().contains("json")) {
                return getXmlRepresentation(usedQuota);
            } else {
                try {
                    return getJsonRepresentation(usedQuota);
                } catch (JSONException e) {
                    return new ResponseEntity<Object>("Caught JSON Exception.", HttpStatus.INTERNAL_SERVER_ERROR);
                }
            }

        } catch (InterruptedException e1) {
            return new ResponseEntity<Object>("Failed to read layer quota : " + layer, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/diskquota", method = RequestMethod.PUT)
    public ResponseEntity<?> doPut(HttpServletRequest request) {
        DiskQuotaConfig config = monitor.getConfig();
        DiskQuotaConfig newConfig = null;
        String reqData = "";
        try {
            StringWriter writer = new StringWriter();
            IOUtils.copy(request.getInputStream(), writer, Charset.defaultCharset());
            reqData = writer.toString();
            if (request.getPathInfo().contains("json")) {
                newConfig = fromJSON(reqData);
                applyDiff(config, newConfig);

                return getJsonRepresentation(config);
            } else {
                newConfig = fromXML(reqData);
                applyDiff(config, newConfig);
                return getXmlRepresentation(config);
            }

        } catch (IOException | JSONException e) {
            return new ResponseEntity<Object>(
                    "Error writing input stream to string", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Applies the set values in {@code newConfig} (the non null ones) to {@code config}
     *
     * @param config
     * @param newConfig
     * @throws IllegalArgumentException as per {@link DiskQuotaConfig#setCacheCleanUpFrequency},
     *     {@link DiskQuotaConfig#setDiskBlockSize}, {@link
     *     DiskQuotaConfig#setMaxConcurrentCleanUps} , {@link DiskQuotaConfig#setCacheCleanUpUnits}
     */
    private void applyDiff(DiskQuotaConfig config, DiskQuotaConfig newConfig)
            throws IllegalArgumentException {
        // apply diff
        if (newConfig != null) {
            if (null != newConfig.isEnabled()) {
                config.setEnabled(newConfig.isEnabled());
            }
            if (null != newConfig.getCacheCleanUpFrequency()) {
                config.setCacheCleanUpFrequency(newConfig.getCacheCleanUpFrequency());
            }
            if (null != newConfig.getDiskBlockSize()) {
                config.setDiskBlockSize(newConfig.getDiskBlockSize());
            }
            if (null != newConfig.getMaxConcurrentCleanUps()) {
                config.setMaxConcurrentCleanUps(newConfig.getMaxConcurrentCleanUps());
            }
            if (null != newConfig.getCacheCleanUpUnits()) {
                config.setCacheCleanUpUnits(newConfig.getCacheCleanUpUnits());
            }
            if (null != newConfig.getGlobalExpirationPolicyName()) {
                config.setGlobalExpirationPolicyName(newConfig.getGlobalExpirationPolicyName());
            }
            if (null != newConfig.getGlobalQuota()) {
                config.setGlobalQuota(newConfig.getGlobalQuota());
            }
            if (null != newConfig.getLayerQuotas()) {
                config.setLayerQuotas(newConfig.getLayerQuotas());
            }
        }
    }

    private DiskQuotaConfig fromJSON(String entity) throws IOException {

        final String text = entity;

        HierarchicalStreamDriver driver = new JettisonMappedXmlDriver();

        XStream xStream = new GeoWebCacheXStream(driver);

        xStream = ConfigLoader.getConfiguredXStream(xStream);

        DiskQuotaConfig configuration;
        StringReader reader = new StringReader(text);
        configuration = ConfigLoader.loadConfiguration(reader, xStream);
        return configuration;
    }

    private DiskQuotaConfig fromXML(String entity) throws IOException {

        final String text = entity;
        StringReader reader = new StringReader(text);
        XStream xstream = ConfigLoader.getConfiguredXStream(new GeoWebCacheXStream());
        DiskQuotaConfig diskQuotaConfig = ConfigLoader.loadConfiguration(reader, xstream);
        return diskQuotaConfig;
    }

    /**
     * Private method for returning a JSON representation of the Statistics
     *
     * @param config
     * @return a {@link ResponseEntity} object
     * @throws JSONException
     */
    private ResponseEntity<?> getJsonRepresentation(DiskQuotaConfig config) throws JSONException {
        JSONObject rep = null;
        try {
            XStream xs =
                            ConfigurationDispatcher.getConfiguredXStreamWithContext(
                            new GeoWebCacheXStream(new JsonHierarchicalStreamDriver()),
                            context,
                            Context.REST);
            JSONObject obj = new JSONObject(xs.toXML(config));
            rep = obj;
        } catch (JSONException jse) {
            log.debug(jse);
        }
        return new ResponseEntity(rep.toString(), HttpStatus.OK);
    }

    /**
     * Private method for returning an XML representation of the Statistics
     *
     * @param config
     * @return a {@link ResponseEntity} object
     * @throws JSONException
     */
    private ResponseEntity<?> getXmlRepresentation(Quota config) {
        XStream xStream = getConfiguredXStream(new GeoWebCacheXStream());
        String xmlText = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + xStream.toXML(config);

        return new ResponseEntity(xmlText, HttpStatus.OK);
    }

    /**
     * Private method for returning a JSON representation of the Statistics
     *
     * @param config
     * @return a {@link ResponseEntity} object
     * @throws JSONException
     */
    private ResponseEntity<?> getJsonRepresentation(Quota config) throws JSONException {
        JSONObject rep = null;
        try {
            XStream xs = xmlConfig.getConfiguredXStreamWithContext(new GeoWebCacheXStream(
                    new JsonHierarchicalStreamDriver()), Context.REST);
            JSONObject obj = new JSONObject(xs.toXML(config));
            rep = obj;
        } catch (JSONException jse) {
            jse.printStackTrace();
        }
        return new ResponseEntity(rep.toString(), HttpStatus.OK);
    }

    /**
     * Private method for retunring an XML representation of the Statistics
     *
     * @param config
     * @return a {@link ResponseEntity} object
     * @throws JSONException
     */
    private ResponseEntity<?> getXmlRepresentation(DiskQuotaConfig config) {
        XStream xStream = getConfiguredXStream(new GeoWebCacheXStream());
        String xmlText = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + xStream.toXML(config);

        return new ResponseEntity(xmlText, HttpStatus.OK);
    }

    /**
     * This method adds to the input {@link XStream} an alias for the CacheStatistics
     *
     * @param xs
     * @return an updated XStream
     */
    public static XStream getConfiguredXStream(XStream xs) {
        xs.setMode(XStream.NO_REFERENCES);
        xs.alias("gwcInMemoryCacheStatistics", CacheStatistics.class);
        return xs;
    }
}

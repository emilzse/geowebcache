package org.geowebcache.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.layer.wms.WMSLayer;

/**
 * Class for handling layers with EHCache
 *
 * @author ez
 * @see Cache
 */
public class EhCacheManager {

    private static Log log = LogFactory.getLog(org.geowebcache.config.EhCacheManager.class);

    private static final Object locker = new Object();

    private static EhCacheManager instance;

    private CacheManager manager;

    private Cache layerCache;

    private boolean enabled;

    private EhCacheManager() {
        manager = CacheManager.create();

        enabled = true;

        // init all caches
        layerCache = initCache(CACHES.LAYERS);

        log.info("Initialized layer cache");
    }

    public static EhCacheManager getInstance() {
        if (instance == null) {
            synchronized (locker) {
                if (instance == null) {
                    instance = new EhCacheManager();
                }
            }
        }

        return instance;
    }

    public void addLayer(WMSLayer layer) {
        if (layer != null && enabled) {
            if (layerCache.getMemoryStoreSize()
                    == layerCache.getCacheConfiguration().getMaxElementsInMemory()) {
                log.warn(
                        String.format(
                                "The limit for cache '%s' is exceeded, it is highly recommended that the limit is increased",
                                layerCache.getName()));
            }

            log.info("Adding layer to cache: " + layer.getName());

            put(layerCache, new Element(layer.getName(), layer));
        }
    }

    public void clearAll() {
        log.info("Clearing all caches..");

        logInfo(manager.getCacheNames());
        manager.clearAll();
        logInfo(manager.getCacheNames());
    }

    public void clear(String cacheName) throws CacheException {
        if (!manager.cacheExists(cacheName)) {
            throw new CacheException("No cache found: name='" + cacheName + "'");
        }

        Cache cache = manager.getCache(cacheName);
        logInfo(cache);
        cache.removeAll();
        logInfo(cache);
    }

    public List<WMSLayer> getLayers() {
        log.info("Enabled LAYERS =  " + enabled);

        if (!enabled) {
            return Collections.emptyList();
        }

        List<WMSLayer> list = new ArrayList<>();

        for (Object key : layerCache.getKeys()) {
            Element e = layerCache.get(key);

            WMSLayer layer = e == null ? null : (WMSLayer) e.getObjectValue();
            if (layer != null) {
                list.add(layer);
            }
        }

        return list;
    }

    public WMSLayer getLayer(String layerName) {
        if (layerName == null || !enabled) {
            return null;
        }

        Element e = layerCache.get(layerName);

        return e == null ? null : (WMSLayer) e.getObjectValue();
    }

    public void removeLayer(String layerName) {
        if (layerName != null && enabled) {
            layerCache.remove(layerName);
        }
    }

    public void shutdown() {
        if (enabled) {
            log.info("Will shutdown manager");
            enabled = false;
            manager.shutdown();
        }
    }

    /**
     * Checks if cache exists, if not creates it
     *
     * @param c
     */
    private Cache initCache(CACHES c) {
        String cacheName = c.cacheName;

        if (!manager.cacheExists(cacheName)) {
            manager.addCache(cacheName);
            log.info("New cache added to manager: name=" + cacheName);
        }

        Cache cache = manager.getCache(cacheName);
        logInfo(cache);

        return cache;
    }

    private void put(Cache cache, Element el) {
        int limit = cache.getCacheConfiguration().getMaxElementsInMemory();
        // if limit is reached log it
        if (cache.getMemoryStoreSize() == limit) {
            log.warn(
                    String.format(
                            "The limit for maxElementsInMemory for cache '%s' is exceeded, it is highly recommended that the limit is increased: limit=%d",
                            cache.getName(), limit));
        }

        cache.put(el);
    }

    private void logInfo(Cache cache) {
        if (cache != null) {
            // cache.getCacheConfiguration().getMaxElementsInMemory()
            log.info(
                    "Cache info: name= "
                            + cache.getName()
                            + " count="
                            + cache.getSize()
                            + " objects="
                            + cache.getStatistics().getObjectCount()
                            + " getMemoryStoreSize="
                            + cache.getMemoryStoreSize()
                            + " maxElementsInCache="
                            + cache.getCacheConfiguration().getMaxElementsInMemory());
        }
    }

    private void logInfo(String... names) {
        for (String name : names) {
            logInfo(manager.getCache(name));
        }
    }

    private enum CACHES {
        LAYERS("layers");

        // name used in ehcache.xml
        public final String cacheName;

        CACHES(final String cacheName) {
            this.cacheName = cacheName;
        }
    }
}

package org.geowebcache.config;

import com.thoughtworks.xstream.XStream;
import java.util.ArrayList;
import java.util.List;
import org.geowebcache.GeoWebCacheExtensions;
import org.geowebcache.config.legends.LegendsRawInfo;
import org.geowebcache.config.legends.LegendsRawInfoConverter;
import org.geowebcache.config.meta.ServiceInformation;
import org.geowebcache.filter.parameters.*;
import org.geowebcache.filter.request.CircularExtentFilter;
import org.geowebcache.filter.request.FileRasterFilter;
import org.geowebcache.filter.request.WMSRasterFilter;
import org.geowebcache.layer.ExpirationRule;
import org.geowebcache.layer.meta.ContactInformation;
import org.geowebcache.layer.meta.LayerMetaInformation;
import org.geowebcache.layer.updatesource.GeoRSSFeedDefinition;
import org.geowebcache.layer.wms.WMSLayer;
import org.geowebcache.mime.FormatModifier;
import org.geowebcache.seed.SeedRequest;
import org.geowebcache.seed.TruncateLayerRequest;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.context.WebApplicationContext;

/**
 * Interface for configuration of layers and gridsets
 *
 * @author ez
 */
public interface ConfigurationDispatcher
        extends TileLayerConfiguration,
                InitializingBean,
                DefaultingConfiguration,
                ServerConfiguration,
                BlobStoreConfiguration,
                GridSetConfiguration {

    public static final String DEFAULT_CONFIGURATION_FILE_NAME = "geowebcache.xml";

    @Deprecated
    XMLGridSet removeGridset(final String gridsetName);

    String getConfigLocation() throws ConfigurationException;

    XStream getConfiguredXStreamWithContext(
            XStream xs, ContextualConfigurationProvider.Context providerContext);

    static XStream getConfiguredXStreamWithContext(
            XStream xs,
            WebApplicationContext context,
            ContextualConfigurationProvider.Context providerContext) {

        {
            // Allow any implementation of these extension points
            xs.allowTypeHierarchy(org.geowebcache.layer.TileLayer.class);
            xs.allowTypeHierarchy(org.geowebcache.filter.parameters.ParameterFilter.class);
            xs.allowTypeHierarchy(org.geowebcache.filter.request.RequestFilter.class);
            xs.allowTypeHierarchy(org.geowebcache.config.BlobStoreInfo.class);
            xs.allowTypeHierarchy(TileLayerConfiguration.class);

            // Allow anything that's part of GWC
            // TODO: replace this with a more narrow whitelist
            xs.allowTypesByWildcard(new String[] {"org.geowebcache.**"});
        }

        xs.setMode(XStream.NO_REFERENCES);

        xs.addDefaultImplementation(ArrayList.class, List.class);

        xs.alias("gwcConfiguration", GeoWebCacheConfiguration.class);
        xs.useAttributeFor(GeoWebCacheConfiguration.class, "xmlns_xsi");
        xs.aliasField("xmlns:xsi", GeoWebCacheConfiguration.class, "xmlns_xsi");
        xs.useAttributeFor(GeoWebCacheConfiguration.class, "xsi_schemaLocation");
        xs.aliasField("xsi:schemaLocation", GeoWebCacheConfiguration.class, "xsi_schemaLocation");
        xs.useAttributeFor(GeoWebCacheConfiguration.class, "xmlns");

        // xs.alias("layers", List.class);
        xs.alias("wmsLayer", WMSLayer.class);

        // configuration for legends info
        xs.registerConverter(new LegendsRawInfoConverter());
        xs.alias("legends", LegendsRawInfo.class);

        xs.alias("blobStores", new ArrayList<BlobStoreInfo>().getClass());
        xs.alias("FileBlobStore", FileBlobStoreInfo.class);
        xs.aliasAttribute(BlobStoreInfo.class, "_default", "default");
        // Alias added to retain XML backwards-compatibility.
        // TODO: Would be nice to be able to use name for consistency
        xs.aliasField("id", BlobStoreInfo.class, "name");

        // These two are for 1.1.x compatibility
        xs.alias("grids", new ArrayList<XMLOldGrid>().getClass());
        xs.alias("grid", XMLOldGrid.class);

        xs.alias("gridSet", XMLGridSet.class);
        xs.alias("gridSubset", XMLGridSubset.class);

        xs.alias("mimeFormats", new ArrayList<String>().getClass());
        xs.alias("formatModifiers", new ArrayList<FormatModifier>().getClass());
        xs.alias("srs", org.geowebcache.grid.SRS.class);
        xs.alias("parameterFilters", new ArrayList<ParameterFilter>().getClass());
        xs.alias("parameterFilter", ParameterFilter.class);
        xs.alias("seedRequest", SeedRequest.class);

        xs.processAnnotations(CaseNormalizer.class);
        xs.processAnnotations(StringParameterFilter.class);
        xs.processAnnotations(RegexParameterFilter.class);
        xs.processAnnotations(FloatParameterFilter.class);
        xs.processAnnotations(IntegerParameterFilter.class);

        xs.alias("formatModifier", FormatModifier.class);

        xs.alias("circularExtentFilter", CircularExtentFilter.class);
        xs.alias("wmsRasterFilter", WMSRasterFilter.class);
        xs.alias("fileRasterFilter", FileRasterFilter.class);

        xs.alias("expirationRule", ExpirationRule.class);
        xs.useAttributeFor(ExpirationRule.class, "minZoom");
        xs.useAttributeFor(ExpirationRule.class, "expiration");

        xs.alias("geoRssFeed", GeoRSSFeedDefinition.class);

        xs.alias("metaInformation", LayerMetaInformation.class);

        xs.alias("serviceInformation", ServiceInformation.class);
        xs.alias("contactInformation", ContactInformation.class);

        xs.omitField(ServiceInformation.class, "citeCompliant");

        xs.processAnnotations(TruncateLayerRequest.class);

        if (context != null) {
            /*
             * Look up XMLConfigurationProvider extension points and let them contribute to the
             * configuration
             */
            List<XMLConfigurationProvider> configExtensions =
                    GeoWebCacheExtensions.extensions(XMLConfigurationProvider.class, context);
            for (XMLConfigurationProvider extension : configExtensions) {
                // Check if the provider is context dependent
                if (extension instanceof ContextualConfigurationProvider
                        &&
                        // Check if the context is applicable for the provider
                        (providerContext == null
                                || !((ContextualConfigurationProvider) extension)
                                        .appliesTo(providerContext))) {
                    // If so, try the next one
                    continue;
                }

                xs = extension.getConfiguredXStream(xs);
            }
        }
        return xs;
    }
}

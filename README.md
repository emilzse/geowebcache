# GeoWebCache - Configure WMS-layers in PostreSQL

This is a extension of GWC providing a solution to configure wms-layers in a PostgreSQL database instead of in geowebcache.xml, enabling support for significant amount of configured layers.

Additional features to master (Additional until merged into master):
* #462: Adds possiblity to configure configuration of layers (WMS only) to PostgreSQL database
  * Gridsets and others configurations are still configured in geowebcache.xml
  * Table contains two columns, name and layer (configured layer is stored as json)
  * Enable by changing gwcConfigDispatcher bean class (geowebcache-core-context.xml)
```xml
<!-- The location of a static configuration file for GeoWebCache. 
       By default this lives in WEB-INF/classes/geowebcache.xml -->
       
  <!-- Change class to org.geowebcache.config.PostgresConfiguration -->
  <bean id="gwcConfigDispatcher" class="org.geowebcache.config.XMLConfiguration">
    <constructor-arg ref="gwcAppCtx" />
    <constructor-arg ref="gwcDefaultStorageFinder" />
    <!-- By default GWC will look for geowebcache.xml in {GEOWEBCACHE_CACHE_DIR},
         if not found will look at GEOSEVER_DATA_DIR/gwc/
         alternatively you can specify an absolute or relative path to a directory
         by replacing the gwcDefaultStorageFinder constructor argument above by the directory
         path, like constructor-arg value="/etc/geowebcache"     
    -->
    <property name="template" value="/geowebcache.xml">
      <description>Set the location of the template configuration file to copy over to the
        cache directory if one doesn't already exist.
      </description>
    </property>
    
  <!-- org.geowebcache.config.PostgresConfiguration options
	<property name="jndiName" value="jdbc/DBDS"> 
    <description>jndiName for DataSource 
       </description> 
     </property>
    <property name="url" value="jdbc:postgresql://host:port/db">
    <description>url to postgres database (mandatory if no jndiName)
      </description>
    </property>
    <property name="user" value="postgres">
    <description>user name to database (mandatory if no jndiName)
      </description>
    </property>
    <property name="password" value="postgres">
    <description>password to database (only if no jndiName)
      </description>
    </property>
    <property name="registerDriver" value="false">
    <description>In case it is needed to register the driver manually (e.g. Tomcat) set value to true
      </description>
    </property>
    <property name="schema" value="gwc">
    <description>schema for layers table in database (defaults to public) (must exists) 
      </description>
    </property>
    <property name="maxBackendRequests" value="64">
    <description>How many concurrent connections can be opened at the same time to generate new cached tiles for wms layers (<= 0 = no limit, default=64). When limit is reached a 302 redirect is response of request, redirecting to origin source
      </description>
    </property>
    -->
  </bean>
```
* #461: Support for specific schema for tilepage and tileset tables (geowebcache-diskquota-jdbc.xml)
```xml
<?xml version="1.0" encoding="utf-8" ?> 
<gwcJdbcConfiguration>
    <dialect>PostgreSQL</dialect> 
    <JNDISource>jdbc/DBDS</JNDISource> 
    <schema>gwc</schema> 
</gwcJdbcConfiguration>
```
* #463: Filter large capabilities documents for WMS/WMTS by providing LAYERS (WMS/WMTS) and SRS (WMTS) parameters
  * http://localhost/geowebcache/service/wms?REQUEST=getcapabilities&LAYERS=layerName
  * http://localhost/geowebcache/service/wmts?REQUEST=getcapabilities&LAYERS=layerName
  * http://localhost/geowebcache/service/wmts?REQUEST=getcapabilities&LAYERS=layerName&SRS=gridSetId
* GetTiles: Will provide how many tiles are missing
  * Mainly useful for fullWMS requests to determine how many subsequent request will be done
  * How: Use a GetMap request and modify with REQUEST=GetTiles

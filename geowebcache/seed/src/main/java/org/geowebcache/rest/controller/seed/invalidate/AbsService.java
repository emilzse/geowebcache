package org.geowebcache.rest.controller.seed.invalidate;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;

public abstract class AbsService implements InitializingBean {

    private static Log log = LogFactory.getLog(AbsService.class);

    /** jndi-name for data source */
    @Value("${gwc.db.jndi-name}")
    private String jndiName;

    /** table for deleted invalidated tiles */
    @Value("${gwc.db.gwc_invalidated_tiles_table_name}")
    protected String tableName;

    protected DataSource dataSource;

    @Override
    public void afterPropertiesSet() throws Exception {
        createDataSource();
    }

    private synchronized void createDataSource() throws NamingException {
        if (dataSource == null) {
            if (jndiName == null) {
                log.error("gwc.db.jndi-name is not defined");
            } else {
                log.info("Creating data source from jndiName: value=" + jndiName);

                try {
                    dataSource = (DataSource) new InitialContext().lookup(jndiName);
                } catch (Exception e) {
                    log.debug(e);
                }
            }
        }
    }
}

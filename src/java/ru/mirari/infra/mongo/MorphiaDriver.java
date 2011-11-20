package ru.mirari.infra.mongo;

import com.google.code.morphia.Morphia;
import com.mongodb.Mongo;
import groovy.util.ConfigObject;
import org.apache.log4j.Logger;
import org.codehaus.groovy.grails.commons.GrailsApplication;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.UnknownHostException;
import java.util.Map;

/**
 * @author Dmitry Kurinskiy
 * @since 08.09.11 15:04
 */
public class MorphiaDriver {
    Morphia morphia = new Morphia();
    Mongo mongo;
    final String dbName;

    @Autowired
    MorphiaDriver(GrailsApplication grailsApplication) throws UnknownHostException {
        ConfigObject config = (ConfigObject) grailsApplication.getConfig().get("mirari");
        config = (ConfigObject) config.get("infra");
        config = (ConfigObject) config.get("mongo");
        Map conf = config;

        String mongoHost = conf.get("host").toString();
        try {
            if (mongoHost != null && !mongoHost.isEmpty()) {
                mongo = new Mongo(mongoHost);
            } else {
                mongo = new Mongo();
            }
        } catch (UnknownHostException e) {
            Logger.getLogger(this.getClass()).error("You should provide valid host in mirari.infra.mongo.host " +
                    "config", e);
            throw e;
        }

        String username = conf.get("username").toString();
        String password = conf.get("password").toString();

        dbName = conf.get("dbName").toString();

        if (!username.isEmpty() || !password.isEmpty()) {
            mongo.getDB(dbName).authenticate(username, password.toCharArray());
        }

        boolean dropDb = (Boolean) conf.get("dropDb");
        if (dropDb) {
            System.out.println("Dropping Mongo database on startup...");
            mongo.getDB(dbName).dropDatabase();
        }
    }
}

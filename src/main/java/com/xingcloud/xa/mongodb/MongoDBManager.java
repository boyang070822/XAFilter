package com.xingcloud.xa.mongodb;

import com.mongodb.*;
import com.xingcloud.basic.conf.ConfigReader;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.net.UnknownHostException;

public class MongoDBManager {
    private static final Logger LOGGER = Logger.getLogger(MongoDBManager.class);
    private static MongoDBManager instance;

    public synchronized static MongoDBManager getInstance() {
        if (instance == null) {
            instance = new MongoDBManager();
        }
        return instance;
    }

    private MongoDBManager() {
            try {
                init();
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
    }

    private void init() throws Exception {
        if (this.mongoClient != null) {
            return;
        }


        host = ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "host");
        port = Integer.parseInt(ConfigReader.getConfig("mongodb_conf.xml",
                "mongodb", "port"));
        DB_NAME = ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "dbname");

        int maxWaitTime = Integer.parseInt(ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "max_wait"));
        int socketTimeout = Integer.parseInt(ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "socket_timeout"));
        int connectionTimeout =  Integer.parseInt(ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "connection_timeout"));
        int t = Integer.parseInt(ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "threads_allowed_to_block_for_connection_multiplier"));

        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        builder.connectTimeout(connectionTimeout);
        builder.threadsAllowedToBlockForConnectionMultiplier(t);
        builder.maxWaitTime(maxWaitTime);
        builder.socketTimeout(socketTimeout);

        MongoClientOptions options = MongoClientOptions.builder().build();
        mongoClient = new MongoClient(new ServerAddress(host, port), options);

        this.DB_NAME = ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "dbname");

        LOGGER.info("[MONGO] - host - " + host);
        LOGGER.info("[MONGO] - port - " + port);
        LOGGER.info("[MONGO] - connectTimeout - " + connectionTimeout);
        LOGGER.info("[MONGO] - threads_allowed_to_block_for_connection_multiplier - " + t);
        LOGGER.info("[MONGO] - maxWaitTime - " + maxWaitTime);
        LOGGER.info("[MONGO] - socketTimeout - " + socketTimeout);
        LOGGER.info("[MONGO] - DB_NAME - " + DB_NAME);
        LOGGER.info("[MONGO] - MongoDB client inited.");
    }

    private MongoClient mongoClient;
    private String host;
    private int port;
    private String DB_NAME;
    private String EVENT_LIST_COLLECTION = "events_list";

    public DB getDB() {
        return mongoClient.getDB(DB_NAME);
    }

    public DBCollection getEventListDBCollection() {
        return getDB().getCollection(EVENT_LIST_COLLECTION);
    }

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

}

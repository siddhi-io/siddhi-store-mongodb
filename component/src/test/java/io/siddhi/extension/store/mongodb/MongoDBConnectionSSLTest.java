/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.extension.store.mongodb;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class MongoDBConnectionSSLTest {
    private static final Log log = LogFactory.getLog(MongoDBConnectionSSLTest.class);

    private static final String baseUri = MongoTableTestUtils.resolveBaseUri();
    private static final String mongoUri =
            String.format("%s?authMechanism=%s", baseUri, AuthenticationMechanism.MONGODB_X509);
    private static MongoClientSettings.Builder mongoClientSettingsBuilder = getOptionsWithSSLEnabled();
    private static String keyStorePath;

    @BeforeClass
    public void init() {
        log.info("== Mongo Table Secure Connection tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== Mongo Table Secure Connection tests completed ==");
    }

    @Test
    public void mongoTableSSLConnectionTest1() throws InterruptedException {
        log.info("mongoTableSSLConnectionTest1 - " +
                "   DASC5-862:Defining a MongoDB table with by defining ssl in mongodburl");

        dropCollection();

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='" + mongoUri + "&ssl=true&sslInvalidHostNameAllowed=true', " +
                "secure.connection='true', " +
                "key.store='" + keyStorePath + "', " +
                "key.store.password='123456', " +
                "trust.store='" + keyStorePath + "', " +
                "trust.store.password='123456')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        Assert.assertEquals(doesCollectionExists(), true, "Definition failed");
    }

    @Test
    public void mongoTableSSLConnectionTest2() {
        log.info("mongoTableSSLConnectionTest2 - " +
                "DASC5-863:Defining a MongoDB table with multiple options defined in mongodburl");

        dropCollection();

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='" + mongoUri + "&ssl=true&sslInvalidHostNameAllowed=true', " +
                "secure.connection='true', " +
                "key.store='" + keyStorePath + "', " +
                "key.store.password='123456', " +
                "trust.store='" + keyStorePath + "', " +
                "trust.store.password='123456')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        Assert.assertEquals(doesCollectionExists(), true, "Definition failed");
    }

    private void dropCollection() {
        MongoClientSettings mongoClientSettings = mongoClientSettingsBuilder.applyConnectionString(
                new ConnectionString(mongoUri)).build();
        try (MongoClient mongoClient = MongoClients.create(mongoClientSettings)) {
            mongoClient.getDatabase("admin").getCollection("FooTable").drop();
        } catch (MongoException e) {
            log.debug("Clearing DB collection failed due to " + e.getMessage(), e);
            throw e;
        }
    }

    private boolean doesCollectionExists() {
        MongoClientSettings mongoClientSettings = mongoClientSettingsBuilder.applyConnectionString(
                new ConnectionString(mongoUri)).build();
        try (MongoClient mongoClient = MongoClients.create(mongoClientSettings)) {
            for (String collectionName : mongoClient.getDatabase("admin").listCollectionNames()) {
                if ("FooTable".equals(collectionName)) {
                    return true;
                }
            }
            return false;
        } catch (MongoException e) {
            log.debug("Checking whether collection was created failed due to" + e.getMessage(), e);
            throw e;
        }
    }

    private static MongoClientSettings.Builder getOptionsWithSSLEnabled() {
        URL trustStoreFile = MongoDBConnectionSSLTest.class.getResource("/mongodb-client.jks");
        keyStorePath = trustStoreFile.getPath();
        File keystoreFile = new File(trustStoreFile.getFile());
        try (InputStream trustStream = new FileInputStream(keystoreFile)) {
            char[] trustPassword = "123456".toCharArray();

            KeyStore trustStore;
            trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(trustStream, trustPassword);

            TrustManagerFactory trustFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(trustStore);
            TrustManager[] trustManagers = trustFactory.getTrustManagers();

            KeyManagerFactory keyManagerFactory =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(trustStore, trustPassword);
            KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();

            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(keyManagers, trustManagers, null);

            MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder();
            mongoClientSettingsBuilder.applyToSslSettings(builder -> {
                builder.enabled(true);
                builder.invalidHostNameAllowed(true);
                builder.context(sslContext);
            });
            return mongoClientSettingsBuilder;
        } catch (FileNotFoundException e) {
            log.debug("Key store file not found for secure connections to mongodb.", e);
        } catch (IOException e) {
            log.debug("I/O Exception in creating trust store for secure connections to mongodb.", e);
        } catch (CertificateException e) {
            log.debug("Certificates in the trust store could not be loaded for secure connections " +
                    "to mongodb.", e);
        } catch (NoSuchAlgorithmException e) {
            log.debug("The algorithm used to check the integrity of the trust store cannot be " +
                    "found.", e);
        } catch (KeyStoreException e) {
            log.debug("Exception in creating trust store, no Provider supports aKeyStoreSpi " +
                    "implementation for the specified type.", e);
        } catch (UnrecoverableKeyException e) {
            log.debug("Key in the keystore cannot be recovered.", e);
        } catch (KeyManagementException e) {
            log.debug("Error in validating the key in the key store/ trust store.", e);
        }
        return null;
    }

}

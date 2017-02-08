package edu.cat.emodbclient;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.client.DatabusClientFactory;
import com.bazaarvoice.emodb.databus.client.DatabusFixedHostDiscoverySource;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.client.DataStoreClientFactory;
import com.bazaarvoice.emodb.sor.client.DataStoreFixedHostDiscoverySource;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolProxies;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.Duration;

/**
 * EmoDB client example.
 *
 * Created docker image for use with this example.
 * <p>
 * Run docker using:<br>
 * <b>docker run --name emodb -d -p 8080:8080 -p 8081:8081 christopwner/emodb
 * </b>
 * </p>
 * <p>
 * Also relies on Solr. Run with docker:<br>
 * <b>docker run --name solr -d -p 8983:8983 -t solr
 * </b><br>
 * <b>docker exec -it --user=solr solr bin/solr create_core -c emodb
 * </b>
 * </p>
 *
 * @author Christopher Towner
 */
public class Client {

    private static final String EMODB = "192.168.99.100:8080";
    private static final String PROGRAM = "example-app";
    private static final String DATA = "sample.json";
    private static final String TABLE = "example:data";
    private static final String KEY = "demo";
    private static final String SOLR = "http://192.168.99.100:8983/solr/emodb";

    private static DataStore dataStore;
    private static SolrClient solr;
    private static Databus databus;
    private static ScheduledFuture pollHandle;

    public static void main(String[] args) throws IOException, SolrServerException {
        //connect to datastore
        MetricRegistry metricRegistry = new MetricRegistry();
        dataStore = ServicePoolBuilder.create(DataStore.class)
                .withHostDiscoverySource(new DataStoreFixedHostDiscoverySource(EMODB))
                .withServiceFactory(DataStoreClientFactory.forCluster("local_default", new MetricRegistry()).usingCredentials(null))
                .withMetricRegistry(metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

        //connect to solr
        solr = new HttpSolrClient.Builder(SOLR).build();

        //create temp map
        Map<String, Object> map;
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try (InputStream stream = loader.getResourceAsStream(DATA)) {
            ObjectMapper mapper = new ObjectMapper();
            map = mapper.readValue(stream,
                    new TypeReference<Map<String, Object>>() {
            });
        }

        //create table and load/index data
        if (!dataStore.getTableExists(TABLE)) {
            createTable();
            loadData(map);
            indexData(map);
        }

        //connect to databus
        databus = ServicePoolBuilder.create(Databus.class)
                .withHostDiscoverySource(new DatabusFixedHostDiscoverySource(EMODB))
                .withServiceFactory(DatabusClientFactory.forCluster("local_default", metricRegistry).usingCredentials(null))
                .withMetricRegistry(metricRegistry)
                .buildProxy(new ExponentialBackoffRetry(5, 50, 1000, TimeUnit.MILLISECONDS));

        //subsribe to data using databus
        subscribeData();

        //allow user to query data
        queryData();

        //cancel polling and close connections
        pollHandle.cancel(true);
        ServicePoolProxies.close(databus);
        ServicePoolProxies.close(dataStore);
    }

    /**
     * Create table on EmoDB. (Milestone #0)
     */
    private static void createTable() {
        Map<String, Object> template = ImmutableMap.of("text", "sample", "color", "blue");
        TableOptions options = new TableOptionsBuilder().setPlacement("ugc_global:ugc").build();
        Audit audit = new AuditBuilder().setProgram(PROGRAM).setLocalHost().build();
        dataStore.createTable(TABLE, options, template, audit);
    }

    /**
     * Load data into EmoDB. (Milestone #0)
     */
    private static void loadData(Map<String, Object> map) throws IOException, SolrServerException {
        Audit audit = new AuditBuilder().setProgram(PROGRAM).setComment("initial submission").setLocalHost().build();
        for (String key : map.keySet()) {
            dataStore.update(TABLE, key, TimeUUIDs.newUUID(), Deltas.literal(map.get(key)), audit);
        }
    }

    /**
     * Index data in Solr. (Milestone #1)
     */
    private static void indexData(Map<String, Object> map) throws SolrServerException, IOException {
        for (String key : map.keySet()) {
            SolrInputDocument document = new SolrInputDocument();
            document.addField("id", key);
            Map obj = (Map) map.get(key);
            document.addField("text", obj.get("text"));
            document.addField("color", obj.get("color"));
            solr.add(document);
        }
        solr.commit();
    }

    /**
     * Query data from Solr. (Milestone #1)
     */
    private static void queryData() {
        Scanner in = new Scanner(System.in);
        String line;
        while (!(line = in.nextLine()).equals("exit")) {
            SolrQuery query = new SolrQuery();
            query.setQuery(line);
            QueryResponse response;
            try {
                response = solr.query(query);
                System.out.println(response);
            } catch (SolrServerException | IOException ex) {
                Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Subscribe to data from EmoDB. (Milestone #2)
     */
    private static void subscribeData() {
        Duration ttl = Duration.standardMinutes(10);
        databus.subscribe(PROGRAM, Conditions.alwaysTrue(), ttl, ttl);
        Runnable poll = () -> {
            PollResult result = databus.poll(PROGRAM, Duration.millis(3000), 10);
            System.out.println(result.getEvents());
            for (Event e : result.getEvents()) {
                indexEvent(e);
            }
        };
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        pollHandle = scheduler.scheduleAtFixedRate(poll, 3, 3, SECONDS);
    }
    
    /**
     * Index an databus event. (Milestone #2)
     */
    private static void indexEvent(Event e) {
        System.out.println(e);
        Map<String, Object> content = e.getContent();
        String key = content.get("~id").toString();
        Map<String, Object> obj = dataStore.get(TABLE, key);
        System.out.println(obj);
    }
}

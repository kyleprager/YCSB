package com.yahoo.ycsb.db;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.CASResponse;

import com.couchbase.client.CouchbaseClient;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class CouchbaseClient2_0 extends DB {

    private CouchbaseClient client;
    private Gson gson = new Gson();

    /**
     * Properties read in:
     * couchbase.uris - comma separated list of URIs to connect the Couchbase client with
     * couchbase.password - password for Couchbase bucket
     */
    @Override
    public void init() throws DBException {
        Properties props = getProperties();
        
        // try to get a comma separated list of URIs to connect to Couchbase with or default to localhost
        // on the default Couchbase port of 8091
        String uris = props.getProperty("couchbase.uris", "http://127.0.0.1:8091/pools");
        List<URI> hosts = new ArrayList<URI>();
        try {
            // split the list of URIs and create List<URI> to pass to the couchbase client
            String[] splitUris = uris.split(",");
            for (String str : splitUris) {
                hosts.add(new URI(str));
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException("URISyntaxException being rethrown from init() method.", e);
        }

        // Name of the Bucket to connect to
        String bucket = "default";

        // Password of the bucket (empty) string if none
        String password = props.getProperty("couchbase.password", "");

        // Connect to the Cluster
        try {
            client = new CouchbaseClient(hosts, bucket, password);
        } catch (IOException e) {
            throw new RuntimeException("IOException being rethrown from init() method.", e);
        }
    }

    @Override
    public void cleanup() throws DBException {
        // Shutting down properly
        client.shutdown();
    }

    @Override
    public int delete(String table, String key) {
        client.delete(key);
        return 0;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {

        // get json values as a Set<Entry<String, String>>
        Set<Entry<String, String>> jsonValues = StringByteIterator.getStringMap(values).entrySet();
        
        // create JSON object from jsonValue set
        JsonObject jsonObject = new JsonObject();
        for (Entry<String, String> entry : jsonValues) {
            jsonObject.addProperty(entry.getKey(), entry.getValue());
        }
        
        // write the jsonObject as a string to Couchbase
        try {
            client.set(key, gson.toJson(jsonObject)).get();
            return 0;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return 1;
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {

        // get the object out of the database
        Object stuff = client.get(key);
        if (stuff != null) {
            return 0;
        }

        return 1;
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set
     * field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        System.out.println("SCAN JUST HAPPENED BUT WE DID NOTHING");
        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        
        // get JSON values as a Set<Entry<String, String>>
        Set<Entry<String, String>> jsonValues = StringByteIterator.getStringMap(values).entrySet();
        
        // create a JsonObject from the jsonValues
        JsonObject jsonObject = new JsonObject();
        for (Entry<String, String> entry : jsonValues) {
            jsonObject.addProperty(entry.getKey(), entry.getValue());
        }
        
        // get the CAS (Check-and-Set) long integer value
        long cas = client.gets(key).getCas();
        
        // try to update the value with the given key, passing in the CAS value
        CASResponse casr = client.cas(key, cas, gson.toJson(jsonObject));

        // return 0 if the document was updated correctly, otherwise throw an informative exception and return 1
        if (casr.equals(CASResponse.OK)) {
            return 0;
        }
        else if (casr.equals(CASResponse.NOT_FOUND)) {
            throw new RuntimeException("Value is not found");
        }
        else if (casr.equals(CASResponse.EXISTS)) {
            throw new RuntimeException("Value exists, but CAS didn't match");
        }
        
        return 1;
        
        // Just doing an insert over the old value appears to be about 1.5x faster than getting the CAS and doing an update
//        return insert(table, key, values);
    }

}

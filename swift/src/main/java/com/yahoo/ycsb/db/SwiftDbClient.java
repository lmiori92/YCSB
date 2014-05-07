/**
 * OpenStack's Swift client binding for YCSB.
 *
 * Lorenzo Miori
 *
 * TODO: once working, submit it to the official repo!!
 *
 */

package com.yahoo.ycsb.db;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import ch.iterate.openstack.swift.AuthenticationResponse;
import ch.iterate.openstack.swift.Client;
import ch.iterate.openstack.swift.Client.AuthVersion;
import ch.iterate.openstack.swift.io.ContentLengthInputStream;
import ch.iterate.openstack.swift.model.Region;
import ch.iterate.openstack.swift.model.StorageObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * MongoDB client for YCSB framework.
 * 
 * Properties to set:
 * 
 * @author lmiori92
 */
public class SwiftDbClient extends DB {

	/** Used to include a field in a response. */
	protected static final Integer INCLUDE = Integer.valueOf(1);

	/** Swift client handler **/
	private Client swiftdb;

	/** Cached authorization **/
	private AuthenticationResponse swiftAuthentication;

	// /** The database to access. */
	// private static String database;

	/**
	 * Count the number of times initialized to teardown on the last
	 * {@link #cleanup()}.
	 */
	private static final AtomicInteger initCount = new AtomicInteger(0);

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	@Override
	public void init() throws DBException {
		// special care for thread safety
		initCount.incrementAndGet();
		synchronized (INCLUDE) {
			if (swiftdb != null) {
				return;
			}

			// initialize swift driver
			// Properties props = getProperties();
			// TODO set a localhost default value !!
			// String url = props.getProperty("swift.url", "10.10.242.6:8080");
			// database = props.getProperty("mongodb.database", "ycsb");
			// String writeConcernType =
			// props.getProperty("mongodb.writeConcern",
			// "safe").toLowerCase();
			// final String maxConnections = props.getProperty(
			// "mongodb.maxconnections", "10");

			try {
				URI authUri = new URL("http", "10.10.242.6", 8080,
						"/auth/v1.0/").toURI();
				System.out.println("Attempting connection with " + authUri);
				swiftdb = new Client(60 * 1000); // ms !!!!!!
				swiftAuthentication = swiftdb.authenticate(AuthVersion.v10,
						authUri, "test:tester", "testing", "tester");
				System.out.println("Swift connection created with " + authUri);
				System.out.println("X-Auth-Token: "
						+ swiftAuthentication.getAuthToken());
				for (Region r : swiftAuthentication.getRegions()) {
					System.out.println(r.getStorageUrl());
				}
			} catch (Exception e1) {
				System.err
						.println("Could not initialize Swift connection pool for Loader: "
								+ e1.toString());
				e1.printStackTrace();
				throw new DBException(
						"Could not initialize Swift connection pool for Loader: "
								+ e1.toString());
				// return;
			}
		}
	}

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
	 */
	@Override
	public void cleanup() throws DBException {
		if (initCount.decrementAndGet() <= 0) {
			try {
				swiftdb.disconnect();
			} catch (Exception e1) {
				System.err.println("Could not close Swift connection pool: "
						+ e1.toString());
				e1.printStackTrace();
				return;
			}
		}
	}

	/**
	 * Delete a record from the database.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int delete(String table, String key) {
		try {
			Region r = (Region) (swiftAuthentication.getRegions()).toArray()[0];
			swiftdb.deleteObject(r, table, key);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 1;
		}
		return 0;
		// com.mongodb.DB db = null;
		// try {
		// db = mongo.getDB(database);
		// db.requestStart();
		// DBCollection collection = db.getCollection(table);
		// DBObject q = new BasicDBObject().append("_id", key);
		// WriteResult res = collection.remove(q, writeConcern);
		// return res.getN() == 1 ? 0 : 1;
		// } catch (Exception e) {
		// System.err.println(e.toString());
		// return 1;
		// } finally {
		// if (db != null) {
		// db.requestDone();
		// }
		// }
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to insert.
	 * @param values
	 *            A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {

		// 0 is the regionId, that actually matters only for .equal purpouses
		try {
			Region r = (Region) (swiftAuthentication.getRegions()).toArray()[0];
			if (!swiftdb.containerExists(r, table)) {
				swiftdb.createContainer(r, table);
				System.out.println("We should create container: " + table
						+ " in " + r);
			}
			Type typeOfMap = new TypeToken<Map<String, ByteIterator>>() {
			}.getType();

			Gson gson = new Gson();
			String json = gson.toJson(values, typeOfMap);

			// region, container, data, type, key
			swiftdb.storeObject(r, table, new ByteArrayInputStream(json
					.toString().getBytes()), "application/json", key,
					new HashMap<String, String>());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 1;
		}

		// com.mongodb.DB db = null;
		// try {
		// db = mongo.getDB(database);
		//
		// db.requestStart();
		//
		// DBCollection collection = db.getCollection(table);
		// DBObject r = new BasicDBObject().append("_id", key);
		// for (String k : values.keySet()) {
		// r.put(k, values.get(k).toArray());
		// }
		// WriteResult res = collection.insert(r, writeConcern);
		// return res.getError() == null ? 0 : 1;
		// } catch (Exception e) {
		// e.printStackTrace();
		// return 1;
		// } finally {
		// if (db != null) {
		// db.requestDone();
		// }
		// }
		return 0;
	}

	/**
	 * Read a record from the database. Each field/value pair from the result
	 * will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to read.
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error or "not found".
	 */
	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		try {
			Region r = (Region) (swiftAuthentication.getRegions()).toArray()[0];
			// String json =
			StringBuilder builder = new StringBuilder();

			ContentLengthInputStream s = swiftdb.getObject(r, table, key);
	        String json = "";
	        while (true)
	        {
	            int x = -1;
	            try
	            {
	                x = s.read();
	            }
	            catch (IOException e)
	            {
	                e.printStackTrace();
	            }
	            if (x != -1)
	                json += String.valueOf((char) x);
	            else
	                break;
	        }

			final Gson gson = new Gson();
			final String string = gson.toJson(json);
			final Type type = new TypeToken<LinkedHashMap<String, ByteIterator>>(){}.getType();
			final Map<String, ByteIterator> map2 = gson.fromJson(string, type);
			result.putAll(map2);

		} catch (IOException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
		// com.mongodb.DB db = null;
		// try {
		// db = mongo.getDB(database);
		//
		// db.requestStart();
		//
		// DBCollection collection = db.getCollection(table);
		// DBObject q = new BasicDBObject().append("_id", key);
		// DBObject fieldsToReturn = new BasicDBObject();
		//
		// DBObject queryResult = null;
		// if (fields != null) {
		// Iterator<String> iter = fields.iterator();
		// while (iter.hasNext()) {
		// fieldsToReturn.put(iter.next(), INCLUDE);
		// }
		// queryResult = collection.findOne(q, fieldsToReturn);
		// } else {
		// queryResult = collection.findOne(q);
		// }
		//
		// if (queryResult != null) {
		// result.putAll(queryResult.toMap());
		// }
		// return queryResult != null ? 0 : 1;
		// } catch (Exception e) {
		// System.err.println(e.toString());
		// return 1;
		// } finally {
		// if (db != null) {
		// db.requestDone();
		// }
		// }
	}

	/**
	 * Update a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key, overwriting any existing values with the same field name.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to write.
	 * @param values
	 *            A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		return 1;
		// com.mongodb.DB db = null;
		// try {
		// db = mongo.getDB(database);
		//
		// db.requestStart();
		//
		// DBCollection collection = db.getCollection(table);
		// DBObject q = new BasicDBObject().append("_id", key);
		// DBObject u = new BasicDBObject();
		// DBObject fieldsToSet = new BasicDBObject();
		// Iterator<String> keys = values.keySet().iterator();
		// while (keys.hasNext()) {
		// String tmpKey = keys.next();
		// fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());
		//
		// }
		// u.put("$set", fieldsToSet);
		// WriteResult res = collection.update(q, u, false, false,
		// writeConcern);
		// return res.getN() == 1 ? 0 : 1;
		// } catch (Exception e) {
		// System.err.println(e.toString());
		// return 1;
		// } finally {
		// if (db != null) {
		// db.requestDone();
		// }
		// }
	}

	/**
	 * Perform a range scan for a set of records in the database. Each
	 * field/value pair from the result will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param startkey
	 *            The record key of the first record to read.
	 * @param recordcount
	 *            The number of records to read
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A Vector of HashMaps, where each HashMap is a set field/value
	 *            pairs for one record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		return 1;
		// com.mongodb.DB db = null;
		// try {
		// db = mongo.getDB(database);
		// db.requestStart();
		// DBCollection collection = db.getCollection(table);
		// // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
		// DBObject scanRange = new BasicDBObject().append("$gte", startkey);
		// DBObject q = new BasicDBObject().append("_id", scanRange);
		// DBCursor cursor = collection.find(q).limit(recordcount);
		// while (cursor.hasNext()) {
		// // toMap() returns a Map, but result.add() expects a
		// // Map<String,String>. Hence, the suppress warnings.
		// HashMap<String, ByteIterator> resultMap = new HashMap<String,
		// ByteIterator>();
		//
		// DBObject obj = cursor.next();
		// fillMap(resultMap, obj);
		//
		// result.add(resultMap);
		// }
		//
		// return 0;
		// } catch (Exception e) {
		// System.err.println(e.toString());
		// return 1;
		// } finally {
		// if (db != null) {
		// db.requestDone();
		// }
		// }

	}

	// /**
	// * TODO - Finish
	// *
	// * @param resultMap
	// * @param obj
	// */
	// @SuppressWarnings("unchecked")
	// protected void fillMap(HashMap<String, ByteIterator> resultMap, DBObject
	// obj) {
	// Map<String, Object> objMap = obj.toMap();
	// for (Map.Entry<String, Object> entry : objMap.entrySet()) {
	// if (entry.getValue() instanceof byte[]) {
	// resultMap.put(entry.getKey(), new ByteArrayByteIterator(
	// (byte[]) entry.getValue()));
	// }
	// }
	// }
}

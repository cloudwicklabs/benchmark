package com.cloudwick.cassandra.dao;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * Movie data access object
 *
 * @author ashrith
 */
public class MovieDAO {
  private static final Logger logger = LoggerFactory.getLogger(MovieDAO.class);
  Session session = null;
  Cluster cluster = null;

  public MovieDAO(Session session) {
    this.session = session;
  }

  public MovieDAO() {
    this("127.0.0.1");
  }

  /**
   * Constructs a session object for a given contact node
   * @param contactNode a cassandra node used to fetch the cluster information from
   */
  public MovieDAO(String contactNode) {
    cluster = Cluster.builder()
        .addContactPoint(contactNode)
        .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
        .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
        .build();

    Metadata metadata = cluster.getMetadata();
    logger.info("Connected to cluster: %s\n", metadata.getClusterName());

    for (Host host: metadata.getAllHosts()) {
      logger.info(
          "DataCenter: %s, Host: %s, Rack: %s\n",
          host.getDatacenter(), host.getAddress(), host.getRack());
    }
    this.connect();
  }

  /**
   * Constructs a session object for a given list of cassandra nodes
   * @param contactNodes list of contact nodes to the cluster information from
   */
  public MovieDAO(List<String> contactNodes) {
    try {
      cluster = Cluster.builder()
          .addContactPoints(contactNodes.toArray(new String[contactNodes.size()]))
          .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
          .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
          .build();

      Metadata metadata = cluster.getMetadata();
      logger.debug("Connected to cluster: {}", metadata.getClusterName());

      for ( Host host : metadata.getAllHosts() ) {
        logger.debug("DataCenter: {}; Host: {}; Rack: {}\n",
            host.getDatacenter(), host.getAddress(), host.getRack());
      }
      this.connect();
    } catch (NoHostAvailableException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } catch (Exception e) {
      logger.error("Exception in CassandraConnection class: {}", e);
    }
  }

  /**
   * Connects to the cassandra cluster using the provided keyspace
   * @param keyspace name of the keyspace to connect to
   */
  public void connect(String keyspace) {
    if (session == null) {
      session = cluster.connect(keyspace);
    }
  }

  public void connect() {
    if (session == null) {
      session = cluster.connect();
    }
  }

  /**
   * Returns a session instance of the connection
   * @return session instance
   */
  public Session getSession() {
    return this.session;
  }

  /**
   * Disconnects from cassandra cluster
   */
  public void close() {
    if (session != null) {
      session.close();
      logger.debug("Successfully closed session");
    }
    if (cluster != null) {
      cluster.close();
      logger.debug("Successfully closed cluster connection");
    }
  }

  /**
   * Creates required schema for movie dataset with provided keyspace name
   * @param keyspace name of the keyspace to create
   * @param repFactor replication factor for the keyspace
   */
  public void createSchema(String keyspace, Integer repFactor) {
    getSession().execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s " +
        "WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': %d};", keyspace, repFactor));
    /*
     * Tables for the keyspace
     */
    getSession().execute(String.format("CREATE TABLE IF NOT EXISTS %s.watch_history (" +
        "cid INT, mid INT, movie_name TEXT, pt INT, ts TIMESTAMP, " +
        "PRIMARY KEY(cid, mid));", keyspace));
    getSession().execute(String.format("CREATE TABLE IF NOT EXISTS %s.customer_rating (" +
        "cid INT, mid INT, movie_name TEXT, customer_name TEXT, rating FLOAT, " +
        "PRIMARY KEY(cid, mid));", keyspace));
    getSession().execute(String.format("CREATE TABLE IF NOT EXISTS %s.customer_queue (" +
        "cid INT, ts TIMESTAMP, customer_name TEXT, mid INT, movie_name TEXT, " +
        "PRIMARY KEY(cid, ts));", keyspace));
    getSession().execute(String.format("CREATE TABLE IF NOT EXISTS %s.movies_genre (" +
        "genre TEXT, release_year INT, mid INT, duration INT, movie_name TEXT, " +
        "PRIMARY KEY(genre, release_year, mid));", keyspace));
  }

  /**
   * Loads data into table watch_history
   * @param keyspace name of the keyspace in which the table resides
   * @param cid customer id
   * @param ts time stamp at which the user stared watching movie in unix epoch timestamp
   * @param pt time at which the user paused the movie (if paused, else 0) in minutes
   * @param mid id of the movie
   * @param mname name of the movie, the user is watching
   * @param async whether to insert in asynchronous mode or regular synchronous mode
   */
  public void loadWatchHistory(String keyspace, Integer cid, String ts,
                               Integer pt, Integer mid, String mname, Boolean async) {
    if (async) {
      getSession().executeAsync(String.format("INSERT INTO %s.watch_history " +
          "(cid, ts, pt, mid, movie_name) VALUES (" +
          "%d, '%s', %d, %d, '%s');", keyspace, cid, ts, pt, mid, mname));
    } else {
      getSession().execute(String.format("INSERT INTO %s.watch_history " +
          "(cid, ts, pt, mid, movie_name) VALUES (" +
          "%d, '%s', %d, %d, '%s');", keyspace, cid, ts, pt, mid, mname));
    }
  }

  /**
   * Batch inserts data into table watch_history
   * @param keyspace name of the keyspace where the table resides
   * @param data hash map of rows to inserts
   * @param async whether to insert in asynchronous mode or regular synchronous mode
   */
  public void batchLoadWatchHistory(String keyspace, HashMap<Integer, List<String>> data, Boolean async) {
    PreparedStatement preparedStatement = getSession().prepare(String.format("INSERT INTO %s.watch_history " +
        "(cid, ts, pt, mid, movie_name) VALUES (?, ?, ?, ?, ?);", keyspace));
    BatchStatement batchStatement = new BatchStatement();

    Iterator<Map.Entry<Integer, List<String>>> iterator = data.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<Integer, List<String>> entry = iterator.next();
      List<String> tableValues = entry.getValue();
      batchStatement.add(preparedStatement.bind(
          Integer.parseInt(tableValues.get(0)),
          new Date(Long.parseLong(tableValues.get(1))),
          Integer.parseInt(tableValues.get(2)),
          Integer.parseInt(tableValues.get(3)),
          tableValues.get(4)
      ));
    }
    if (async)
      getSession().executeAsync(batchStatement);
    else
      getSession().execute(batchStatement);
    logger.debug("Number of records inserted into watch_history: " + data.size());
  }

  /**
   * Loads data into table customer_rating
   * @param keyspace name of the keyspace in which the table resides
   * @param cid customer id
   * @param mid movie id
   * @param mname movie name
   * @param cname customer name
   * @param rating rating given by the customer
   * @param async whether to insert in asynchronous mode or regular synchronous mode
   */
  public void loadCustomerRatings(String keyspace, Integer cid, Integer mid,
                                  String mname, String cname, Float rating, Boolean async) {
    if (async) {
      getSession().executeAsync(String.format("INSERT INTO %s.customer_rating " +
          "(cid, mid, movie_name, customer_name, rating) VALUES (" +
          "%d, %d, '%s', '%s', %f);", keyspace, cid, mid, mname, cname, rating));
    } else {
      getSession().execute(String.format("INSERT INTO %s.customer_rating " +
          "(cid, mid, movie_name, customer_name, rating) VALUES (" +
          "%d, %d, '%s', '%s', %f);", keyspace, cid, mid, mname, cname, rating));
    }
  }

  /**
   * Batch loads data into table customer_rating, where batch size is the size of the hash map passed
   * @param keyspace name of the keyspace in which the table resides
   * @param data hash map of the rows to insert
   * @param async whether to insert in asynchronous mode or regular synchronous mode
   */
  public void batchLoadCustomerRatings(String keyspace, HashMap<Integer, List<String>> data, Boolean async) {
    PreparedStatement preparedStatement = getSession().prepare(String.format("INSERT INTO %s.customer_rating " +
        "(cid, mid, movie_name, customer_name, rating) VALUES (?, ?, ?, ?, ?);", keyspace));
    BatchStatement batchStatement = new BatchStatement();

    Iterator<Map.Entry<Integer, List<String>>> iterator = data.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<Integer, List<String>> entry = iterator.next();
      List<String> tableValues = entry.getValue();
      batchStatement.add(preparedStatement.bind(
          Integer.parseInt(tableValues.get(0)),
          Integer.parseInt(tableValues.get(1)),
          tableValues.get(2),
          tableValues.get(3),
          Float.parseFloat(tableValues.get(4))
      ));
    }
    if (async)
      getSession().executeAsync(batchStatement);
    else
      getSession().execute(batchStatement);
    logger.debug("Number of records inserted into customer_rating: " + data.size());
  }

  /**
   * Loads data into table customer_queue
   * @param keyspace name of the keyspace in which the table resides
   * @param cid customer id
   * @param ts movie id
   * @param cname customer name
   * @param mid movie id
   * @param mname movie name
   * @param async whether to insert in asynchronous mode or regular synchronous mode
   */
  public void loadCustomerQueue(String keyspace, Integer cid, String ts,
                                String cname, Integer mid, String mname, Boolean async) {
    if (async) {
      getSession().executeAsync(String.format("INSERT INTO %s.customer_queue " +
          "(cid, ts, customer_name, mid, movie_name) VALUES (" +
          "%d, '%s', '%s', %d, '%s');", keyspace, cid, ts, cname, mid, mname));
    } else {
      getSession().execute(String.format("INSERT INTO %s.customer_queue " +
          "(cid, ts, customer_name, mid, movie_name) VALUES (" +
          "%d, '%s', '%s', %d, '%s');", keyspace, cid, ts, cname, mid, mname));
    }
  }

  /**
   * Batch loads data into customer_queue, where the batch size is the size of the map passed
   * @param keyspace name of the keyspace in which the table resides
   * @param data map of the rows to insert
   * @param async whether to insert in asynchronous mode or regular synchronous mode
   */
  public void batchLoadCustomerQueue(String keyspace, HashMap<Integer, List<String>> data, Boolean async) {
    PreparedStatement preparedStatement = getSession().prepare(String.format("INSERT INTO %s.customer_queue " +
        "(cid, ts, customer_name, mid, movie_name) VALUES (?, ?, ?, ?, ?);", keyspace));
    BatchStatement batchStatement = new BatchStatement();

    Iterator<Map.Entry<Integer, List<String>>> iterator = data.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<Integer, List<String>> entry = iterator.next();
      List<String> tableValues = entry.getValue();
      batchStatement.add(preparedStatement.bind(
          Integer.parseInt(tableValues.get(0)),
          new Date(Long.parseLong(tableValues.get(1))),
          tableValues.get(2),
          Integer.parseInt(tableValues.get(3)),
          tableValues.get(4)
      ));
    }
    if (async)
      getSession().executeAsync(batchStatement);
    else
      getSession().execute(batchStatement);
    logger.debug("Number of records inserted into customer_queue: " + data.size());
  }

  /**
   * Loads data into table customer_queue
   * @param keyspace name of the keyspace in which the table resides
   * @param genre genre of the movie
   * @param ryear movie's release year
   * @param mid id of the movie
   * @param dur duration(run time) of the movie
   * @param mname movie name
   * @param async whether to insert in asynchronous mode or regular synchronous mode
   */
  public void loadMovieGenre(String keyspace, String genre, Integer ryear,
                             Integer mid, Integer dur, String mname, Boolean async) {
    if (async) {
      getSession().executeAsync(String.format("INSERT INTO %s.movies_genre " +
          "(genre, release_year, mid, duration, movie_name) VALUES (" +
          "'%s', %d, %d, %d, '%s');", keyspace, genre, ryear, mid, dur, mname));
    } else {
      getSession().execute(String.format("INSERT INTO %s.movies_genre " +
          "(genre, release_year, mid, duration, movie_name) VALUES (" +
          "'%s', %d, %d, %d, '%s');", keyspace, genre, ryear, mid, dur, mname));
    }
  }

  /**
   * Batch loads data into customer_queue, where the batch size is the size of the map passed
   * @param keyspace name of the keyspace in which the table resides
   * @param data map of the rows to insert
   * @param async whether to insert in asynchronous mode or regular synchronous mode
   */
  public void batchLoadMovieGenre(String keyspace, HashMap<Integer, List<String>> data, Boolean async) {
    PreparedStatement preparedStatement = getSession().prepare(String.format("INSERT INTO %s.movies_genre " +
        "(genre, release_year, mid, duration, movie_name) VALUES (?, ?, ?, ?, ?);", keyspace));
    BatchStatement batchStatement = new BatchStatement();

    Iterator<Map.Entry<Integer, List<String>>> iterator = data.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<Integer, List<String>> entry = iterator.next();
      List<String> tableValues = entry.getValue();
      batchStatement.add(preparedStatement.bind(
          tableValues.get(0),
          Integer.parseInt(tableValues.get(1)),
          Integer.parseInt(tableValues.get(2)),
          Integer.parseInt(tableValues.get(3)),
          tableValues.get(4)
      ));
    }
    if (async)
      getSession().executeAsync(batchStatement);
    else
      getSession().execute(batchStatement);
    logger.debug("Number of records inserted into movies_genre: " + data.size());
  }

  public void findCQLByQuery(String CQL) {
    Statement cqlQuery = new SimpleStatement(CQL);
    cqlQuery.setConsistencyLevel(ConsistencyLevel.ONE);
    cqlQuery.enableTracing();

    ResultSet resultSet = getSession().execute(cqlQuery);

    // Get the columns returned by the query
    ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
     // logger.debug(columnDefinitions.toString());
    for (Row row: resultSet) {
      logger.debug(row.toString());
    }
  }

  /**
   * Drops the specified keyspace
   * @param keyspace to drop
   */
  public void dropSchema(String keyspace) {
    getSession().execute("DROP KEYSPACE " + keyspace);
    logger.info("Finished dropping " + keyspace + " keyspace.");
  }

  /**
   * Drops a specified table from a given keyspace
   * @param keyspace to drop the table from
   * @param table to delete
   */
  public void dropTable(String keyspace, String table) {
    getSession().execute("DROP TABLE IF EXISTS " + keyspace + "." + table);
    logger.info("Finished dropping table " + table + " from " + keyspace + " keyspace.");
  }
}

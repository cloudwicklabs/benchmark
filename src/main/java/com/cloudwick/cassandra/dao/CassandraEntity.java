package com.cloudwick.cassandra.dao;

import java.util.HashMap;

/**
 * Object wrapper for cassandra Entity
 *
 * @author ashrith
 */
public abstract class CassandraEntity {

  protected String key;
  protected HashMap<String, Object> attributes;

  public CassandraEntity() {
    attributes = new HashMap<String, Object>();
  }

  public HashMap<String, Object> getAttributes() {
    return attributes;
  }

  public void setAttributes(HashMap<String, Object> attributes) {
    this.attributes = attributes;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }
}
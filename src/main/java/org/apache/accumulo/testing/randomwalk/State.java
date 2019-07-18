/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.testing.randomwalk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * A structure for storing state kept during a test. This class is not thread-safe.
 */
public class State {

  private HashMap<String,Object> stateMap = new HashMap<>();
  private List<String> tables = new ArrayList<>();
  private List<String> namespaces = new ArrayList<>();
  private List<String> users = new ArrayList<>();
  private Random random = new Random();

  /**
   * Creates new empty state.
   */
  public State() {}

  /**
   * Sets a state object.
   *
   * @param key
   *          key for object
   * @param value
   *          object
   */
  public void set(String key, Object value) {
    stateMap.put(key, value);
  }

  /**
   * Resets the Random object.
   */
  public void setRandom(Random rand) {
    this.random = rand;
  }

  /**
   * Gets the random object.
   */
  public Random getRandom() {
    return random;
  }

  /**
   * Removes a state object.
   *
   * @param key
   *          key for object
   */
  public void remove(String key) {
    stateMap.remove(key);
  }

  /**
   * Gets a state object.
   *
   * @param key
   *          key for object
   * @return value object
   * @throws RuntimeException
   *           if state object is not present
   */
  public Object get(String key) {
    if (stateMap.containsKey(key) == false) {
      throw new RuntimeException("State does not contain " + key);
    }
    return stateMap.get(key);
  }

  public List<String> getTableNames() {
    return tables;
  }

  public void addTable(String tableName) {
    tables.add(tableName);
  }

  public void addNamespace(String ns) {
    namespaces.add(ns);
  }

  public String getRandomNamespace() {
    return namespaces.get(random.nextInt(namespaces.size()));
  }

  /**
   * Gets a random table name
   */
  public String getRandomTableName() {
    return tables.get(random.nextInt(tables.size()));
  }

  public void addUser(String userName) {
    users.add(userName);
  }

  public String getRandomUser() {
    return users.get(random.nextInt(users.size()));
  }

  /**
   * Gets a state object, returning null if it is absent.
   *
   * @param key
   *          key for object
   * @return value object, or null if not present
   */
  public Object getOkIfAbsent(String key) {
    return stateMap.get(key);
  }

  /**
   * Gets the map of state objects. The backing map for state is returned, so changes to it affect
   * the state.
   *
   * @return state map
   */
  HashMap<String,Object> getMap() {
    return stateMap;
  }

  /**
   * Gets a state object as a string.
   *
   * @param key
   *          key for object
   * @return value as string
   * @throws ClassCastException
   *           if the value object is not a string
   */
  public String getString(String key) {
    return (String) stateMap.get(key);
  }

  /**
   * Gets a state object as an integer.
   *
   * @param key
   *          key for object
   * @return value as integer
   * @throws ClassCastException
   *           if the value object is not an integer
   */
  public Integer getInteger(String key) {
    return (Integer) stateMap.get(key);
  }

  /**
   * Gets a state object as a long.
   *
   * @param key
   *          key for object
   * @return value as long
   * @throws ClassCastException
   *           if the value object is not a long
   */
  public Long getLong(String key) {
    return (Long) stateMap.get(key);
  }
}

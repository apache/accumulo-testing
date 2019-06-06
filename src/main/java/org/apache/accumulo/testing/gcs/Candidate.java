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

package org.apache.accumulo.testing.gcs;

import java.util.Comparator;

import com.google.common.base.Objects;

public class Candidate implements Comparable<Candidate> {
  public final long clientId;
  public final long groupId;
  public final long itemId;

  Candidate(long clientId, long groupId, long itemId) {
    this.clientId = clientId;
    this.groupId = groupId;
    this.itemId = itemId;
  }

  public long getClientId() {
    return clientId;
  }

  public long getGroupId() {
    return groupId;
  }

  public long getItemId() {
    return itemId;
  }

  private static Comparator<Candidate> COMPARATOR = Comparator.comparingLong(Candidate::getClientId)
      .thenComparingLong(Candidate::getGroupId).thenComparingLong(Candidate::getItemId);

  @Override
  public int compareTo(Candidate o) {
    return COMPARATOR.compare(this, o);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(clientId, groupId, itemId);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Candidate) {
      Item oi = (Item) o;
      return clientId == oi.clientId && groupId == oi.groupId && itemId == oi.itemId;
    }

    return false;
  }

  public Item item() {
    return new Item(clientId, groupId, itemId);
  }
}

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

public class ItemRef implements Comparable<ItemRef> {
  public final int bucket;
  public final long clientId;
  public final long groupId;
  public final long itemId;

  ItemRef(int bucket, long clientId, long groupId, long itemId) {
    this.bucket = bucket;
    this.clientId = clientId;
    this.groupId = groupId;
    this.itemId = itemId;
  }

  ItemRef(int bucket, Item item) {
    this(bucket, item.clientId, item.groupId, item.itemId);
  }

  public Item item() {
    return new Item(clientId, groupId, itemId);
  }

  @Override
  public int compareTo(ItemRef o) {
    int cmp = Long.compare(bucket, o.bucket);
    if (cmp == 0) {
      cmp = item().compareTo(o.item());
    }

    return cmp;
  }

  @Override
  public String toString() {
    return "bucket:" + Persistence.toHex(bucket) + " " + item();
  }
}

/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package org.blobit.core.api;

import java.util.List;
import java.util.Objects;

/**
 * Metadata about an object with a custom name. A named object can span multiple
 * simple objects in case of concatenation/append operations.
 *
 * @author eolivelli
 * @see ObjectMetadata
 */
public final class NamedObjectMetadata {

    private final String name;
    private final long size;
    private final List<ObjectMetadata> objects;

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
            value = "EI_EXPOSE_REP2")
    public NamedObjectMetadata(String name, long size,
            List<ObjectMetadata> objects) {
        this.name = name;
        this.size = size;
        this.objects = objects;
    }

    public String getName() {
        return name;
    }

    public long getSize() {
        return size;
    }

    public ObjectMetadata getObject(int index) {
        return objects.get(index);
    }

    public int getNumObjects() {
        return objects.size();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.name);
        hash = 97 * hash + (int) (this.size ^ (this.size >>> 32));
        hash = 97 * hash + Objects.hashCode(this.objects);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final NamedObjectMetadata other = (NamedObjectMetadata) obj;
        if (this.size != other.size) {
            return false;
        }
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.objects, other.objects)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "{" + "name=" + name + ", size=" + size + ", objects=" + objects + '}';
    }

}

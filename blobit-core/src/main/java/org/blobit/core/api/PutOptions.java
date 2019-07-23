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

/**
 * Configure the behaviour of the Put operation
 */
public final class PutOptions {

    /**
     * Default options.
     */
    public static final PutOptions DEFAULT_OPTIONS = new PutOptions(false, false);

    /**
     * Overwrite.
     */
    public static final PutOptions OVERWRITE = new PutOptions(true, false);


    /**
     * Append.
     */
    public static final PutOptions APPEND = new PutOptions(false, true);


    /**
     * Start building a new PutOptions object.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    private final boolean overwrite;
    private final boolean append;

    private PutOptions(boolean overwrite, boolean append) {
        this.overwrite = overwrite;
        this.append = append;
        if (overwrite && append) {
            throw new IllegalArgumentException("Cannot append and overwrite");
        }
    }

    public static final class Builder {

        private boolean overwrite = false;
        private boolean append = false;

        private Builder() {
        }

        /**
         * If an object with the same name already exists then overwrite it.
         *
         * @param value
         * @return the builder
         */
        public Builder overwrite(boolean value) {
            this.overwrite = value;
            return this;
        }

        /**
         * If an object with the same name already exists then append to it.
         *
         * @param value
         * @return the builder
         */
        public Builder append(boolean value) {
            this.append = value;
            return this;
        }

        public PutOptions build() {
            return new PutOptions(overwrite, append);
        }

    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public boolean isAppend() {
        return append;
    }

}

/**
 * Copyright (C) 2010-2011, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * The software in this package is published under the terms of the
 * CDDL license a copy of which has been included with this distribution
 * in the license.txt file.
 */
package org.fusesource.hawtdb.transaction;

public class Callback<T> {

    public void failure(Throwable value) {
    }

    public void success(T value) {
    }

}

/*
 * Copyright (c) 2018, little-pan, All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 */
package io.conio.util;

/**
 * <p>
 *     Runtime utils.
 * </p>
 * @author little-pan
 * @since 2018-08-19
 */
public final class RtUtils {

    private RtUtils(){}

    public final static Runtime RT = Runtime.getRuntime();

    public final static int PROCESSORS = RT.availableProcessors();

}

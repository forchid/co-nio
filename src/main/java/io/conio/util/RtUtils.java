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

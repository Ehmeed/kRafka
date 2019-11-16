package org.ehmeed.kRafka.logging

import kotlin.reflect.KClass
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Extensions for lazy logging a message that's expensive to calculate
 */

inline fun Logger.error(throwable: Throwable? = null, crossinline message: () -> String) {
    if (isErrorEnabled) error(message(), throwable)
}

inline fun Logger.warn(throwable: Throwable? = null, crossinline message: () -> String) {
    if (isWarnEnabled) warn(message(), throwable)
}

inline fun Logger.info(throwable: Throwable? = null, crossinline message: () -> String) {
    if (isInfoEnabled) info(message(), throwable)
}

inline fun Logger.debug(throwable: Throwable? = null, crossinline message: () -> String) {
    if (isDebugEnabled) debug(message(), throwable)
}

inline fun Logger.trace(throwable: Throwable? = null, crossinline message: () -> String) {
    if (isTraceEnabled) trace(message(), throwable)
}

/**
 * Use for top level logger
 *
 * private val log = logger(Foo::class)
 */
fun logger(type: KClass<*>): Lazy<Logger> {
    return lazy { LoggerFactory.getLogger(type.java) }
}

/**
 * Use for class level logger
 *
 * class Example {
 *      private val log by logger()
 * }
 */
fun <R : Any> R.logger(): Lazy<Logger> {
    return lazy {
        LoggerFactory.getLogger((this.javaClass.enclosingClass?.takeIf {
            this.javaClass.enclosingClass.kotlin.java == this.javaClass
        } ?: this.javaClass).name)
    }
}

/**
 * Use for toplevel logger without class
 *
 * private val log by logger {}
 */
fun logger(lambda: () -> Unit): Lazy<Logger> =
    lazy { LoggerFactory.getLogger(lambda.javaClass.name.replace(Regex("""\$.*$"""), "")) }

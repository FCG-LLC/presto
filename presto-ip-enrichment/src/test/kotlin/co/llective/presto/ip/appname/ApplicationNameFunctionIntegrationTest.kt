package co.llective.presto.ip.appname

import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

val WKP = 0x64ff9b0000000000L

class ApplicationNameFunctionIntegrationTest {
    @Nested
    inner class WithoutPort {
        @Nested
        inner class Ipv4 {
            @Test
            fun resolvesLocalhostAddress() {
                val resolver = ApplicationNameResolver
                val lowLong = 2130706433L //127.0.0.1
                val appName = resolver.getApplicationName(WKP, lowLong)
                assertEquals("local", appName)
            }

            @Test
            fun resolvesSomeOtherLocalAddress() {
                val resolver = ApplicationNameResolver
                val lowLong = 2130706532L //127.0.0.100
                val appName = resolver.getApplicationName(WKP, lowLong)
                assertEquals("local", appName)
            }

            @Test
            fun resolvesGoogleDnsServer() {
                val resolver = ApplicationNameResolver
                val lowLong = 134217728L //8.8.8.8
                val appName = resolver.getApplicationName(WKP, lowLong)
                assertEquals("ip4_google.com", appName)
            }
        }

        @Nested
        inner class Ipv6 {
            @Test
            fun resolvesGoogleDnsServer() {
                val resolver = ApplicationNameResolver
                val highLong = 2306204062558715904L
                val lowLong = 34952L //2001:4860:4860::8888
                val appName = resolver.getApplicationName(highLong, lowLong)
                assertEquals("ip6_google.com", appName)
            }
        }

        @Nested
        inner class Unknown {
            @Test
            fun returnsNullWhenUnknownAddress() {
                val resolver = ApplicationNameResolver
                val highLong = 1L
                val lowlong = 1L
                val appName = resolver.getApplicationName(highLong, lowlong)
                assertNull(appName)
            }
        }
    }

    @Nested
    inner class WithPort {
        @Nested
        inner class Ipv4 {
            @Test
            fun returnsNameFromSubnetWhenExist() {
                val resolver = ApplicationNameResolver
                val lowLong = 2130706433L //127.0.0.1
                val applicationName = resolver.getApplicationName(WKP, lowLong, 0)
                assertEquals("local", applicationName)
            }

            @Test
            fun returnsPortApplicationWhenSubnetNotFound() {
                val resolver = ApplicationNameResolver
                val lowLong = 1L
                val applicationName = resolver.getApplicationName(WKP, lowLong, 0)
                assertEquals("reserved", applicationName)
            }

            @Test
            fun returnsNullWhenNotKnownPortNorSubnet() {
                val resolver = ApplicationNameResolver
                val lowLong = 1L
                val applicationName = resolver.getApplicationName(WKP, lowLong, 50000)
                assertNull(applicationName)
            }
        }

        @Nested
        inner class Ipv6 {
            @Test
            fun returnsNameFromSubnetWhenExist() {
                val resolver = ApplicationNameResolver
                val highLong = 2306204062558715904L
                val lowLong = 34952L //2001:4860:4860::8888
                val applicationName = resolver.getApplicationName(highLong, lowLong, 0)
                assertEquals("ip6_google.com", applicationName)
            }

            @Test
            fun returnsPortApplicationWhenSubnetNotFound() {
                val resolver = ApplicationNameResolver
                val highLong = 306204062558715904L
                val lowLong = 1L
                val applicationName = resolver.getApplicationName(highLong, lowLong, 1)
                assertEquals("tcpmux", applicationName)
            }

            @Test
            fun returnsNullWhenNotKnownPortNorSubnet() {
                val resolver = ApplicationNameResolver
                val highLong = 306204062558715904L
                val lowLong = 1L
                val applicationName = resolver.getApplicationName(highLong, lowLong, 50000)
                assertNull(applicationName)
            }
        }
    }
}

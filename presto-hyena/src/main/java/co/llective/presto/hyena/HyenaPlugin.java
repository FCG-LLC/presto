/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.llective.presto.hyena;

import co.llective.presto.hyena.enrich.appname.ApplicationNameFunctions;
import co.llective.presto.hyena.enrich.geoip.GeoIpFunctions;
import co.llective.presto.hyena.enrich.ipstring.IpToStringFunction;
import co.llective.presto.hyena.enrich.topdisco.InterfaceNameFunction;
import co.llective.presto.hyena.enrich.topdisco.RouterNameFunction;
import co.llective.presto.hyena.enrich.username.UserNameFunction;
import co.llective.presto.hyena.types.U64BigIntOperators;
import co.llective.presto.hyena.types.U64IntOperators;
import co.llective.presto.hyena.types.U64Operators;
import co.llective.presto.hyena.types.U64Type;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.Set;

public class HyenaPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new HyenaConnectorFactory());
    }

    @Override
    public Iterable<Type> getTypes()
    {
        return Lists.newArrayList(U64Type.U_64_TYPE);
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(U64Operators.class)
                .add(U64BigIntOperators.class)
                .add(U64IntOperators.class)
                .add(UserNameFunction.class)
                .add(GeoIpFunctions.class)
                .add(IpToStringFunction.class)
                .add(ApplicationNameFunctions.class)
                .add(InterfaceNameFunction.class)
                .add(RouterNameFunction.class)
                .build();
    }
}

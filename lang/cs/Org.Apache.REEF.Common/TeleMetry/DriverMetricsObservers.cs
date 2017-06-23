using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Common.Telemetry
{
    [NamedParameter]
    public class DriverMetricsObservers : Name<ISet<IObserver<IDriverMetrics>>>
    {
    }

    public class DriverMetricsObserverConfigurationModule : ConfigurationModuleBuilder
    {
        public static ConfigurationModule ConfigurationModule = new DriverMetricsObserverConfigurationModule()
            .BindSetEntry<DriverMetricsObservers, MetricsService, IObserver<IDriverMetrics>>(GenericType<DriverMetricsObservers>.Class, GenericType<MetricsService>.Class)
            .Build();
    }
}

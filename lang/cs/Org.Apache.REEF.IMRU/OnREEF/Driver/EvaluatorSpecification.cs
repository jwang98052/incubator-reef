using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    internal sealed class EvaluatorSpecification
    {
        internal EvaluatorSpecification(int megabytes, int core)
        {
            Megabytes = megabytes;
            Core = core;
        }

        internal int Megabytes { get; set; }

        internal int Core { get; set; }
    }
}

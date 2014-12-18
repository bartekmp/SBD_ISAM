using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ISAM
{
    public class NegativeDeltaException : Exception
    {
        public NegativeDeltaException(string message = "Negative delta!")
            : base(message)
        {
        }

        public NegativeDeltaException(string message, Exception inner)
            : base(message, inner)
        {
        }

    }

    public class PageFaultException : Exception
    {
        public PageFaultException(string message = "Page fault")
        {

        }
    }
}

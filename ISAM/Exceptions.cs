using System;

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
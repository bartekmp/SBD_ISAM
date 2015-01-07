using System;
using System.Collections.Generic;
using System.Linq;

namespace ISAM
{
    public class Randoms
    {
        private static readonly Random random = new Random();

        // Note, max is exclusive here!
        public static List<int> GenerateRandom(int count, int min = 0, int max = Int32.MaxValue)
        {
            //  initialize set S to empty
            //  for J := N-M + 1 to N do
            //    T := RandInt(1, J)
            //    if T is not in S then
            //      insert T in S
            //    else
            //      insert J in S
            //
            // adapted for C# which does not have an inclusive Next(..)
            // and to make it from configurable range not just 1.

            if (max <= min || count < 0 ||
                // max - min > 0 required to avoid overflow
                (count > max - min && max - min > 0))
            {
                // need to use 64-bit to support big ranges (negative min, positive max)
                throw new ArgumentOutOfRangeException("Range " + min + " to " + max +
                                                      " (" + (max - (Int64) min) + " values), or count " + count +
                                                      " is illegal");
            }

            // generate count random values.
            var candidates = new HashSet<int>();

            // start count values before max, and end at max
            for (int top = max - count; top < max; top++)
            {
                // May strike a duplicate.
                // Need to add +1 to make inclusive generator
                // +1 is safe even for MaxVal max value because top < max
                if (!candidates.Add(random.Next(min, top + 1)))
                {
                    // collision, add inclusive max.
                    // which could not possibly have been added before.
                    candidates.Add(top);
                }
            }

            // load them in to a list, to sort
            List<int> result = candidates.ToList();

            // shuffle the results because HashSet has messed
            // with the order, and the algorithm does not produce
            // random-ordered results (e.g. max-1 will never be the first value)
            for (int i = result.Count - 1; i > 0; i--)
            {
                int k = random.Next(i + 1);
                int tmp = result[k];
                result[k] = result[i];
                result[i] = tmp;
            }
            return result;
        }

        public static Tuple<long, long, long> GenerateCoefficients()
        {
            long a = random.Next(-10000, 10000);
            long b = random.Next(-10000, 10000);
            long c = random.Next(-10000, 10000);
            while (b*b - 4*a*c <= 1e-15)
            {
                a = random.Next(-10000, 10000);
                b = random.Next(-10000, 10000);
                c = random.Next(-10000, 10000);
            }
            return new Tuple<long, long, long>(a, b, c);
        }

        public static List<long> RandomKeys(int count, long min, long max)
        {
            var list = new List<long>();
            var rnd = new Random();
            long key = rnd.Next((int) min, (int) max);

            for (int i = 0; i < count; i++)
            {
                while (list.Contains(key))
                    key = rnd.Next((int) min, (int) max);
                list.Add(key);
            }

            return list;
        }
    }
}
using System;
using System.Collections.Generic;

/*
 * F mainFile index mainPageSize indexPageSize - read index from "index" file
 * N mainFile index mainPageSize indexPageSize - create new index in "index" file, with given page size
 * A key [A B C] - add record with given key, optional coefficients
 * U key A B C - update record value
 * R key - remove record with given key
 * G key - get record with given key
 * PI - print BTree
 * PF - print main file in order
 * PA - print all without ordering
 * RO - reorganize
 * TEST N - add N random entries
 */

namespace ISAM
{
    public static class Program
    {
        public static long MainReads = 0L, MainWrites = 0L, Operations = 0L;


        private static void Main(string[] args)
        {
            Index index = null;
            string readLine;
            while ((readLine = Console.ReadLine()) != null)
            {
                string[] split = readLine.Split(new[] {' '});
                switch (split[0])
                {
                    case "F":
                        if (index == null)
                        {
                            index = new Index(split[1], split[2]);
                        }
                        break;
                    case "N":
                        if (index == null)
                        {
                            index = new Index(split[1], split[2], Int32.Parse(split[3]), Index.Mode.New);
                        }
                        break;
                    case "A":
                        if (index != null)
                        {
                            Operations++;
                            if (split.Length == 5)
                                index.Add(new Record(Int64.Parse(split[1]), Int64.Parse(split[2]), Int64.Parse(split[3]),
                                    Int64.Parse(split[4])));
                            if (split.Length == 2)
                            {
                                Tuple<long, long, long> coeffs = Randoms.GenerateCoefficients();
                                index.Add(new Record(Int64.Parse(split[1]), coeffs.Item1, coeffs.Item2, coeffs.Item3));
                            }
                            PrintInfo();
                        }
                        break;
                    case "U":
                        if (index != null)
                        {
                            Operations++;
                            index.Update(new Record(Int64.Parse(split[1]), Int64.Parse(split[2]), Int64.Parse(split[3]),
                                Int64.Parse(split[4])));
                            PrintInfo();
                        }
                        break;
                    case "R":
                        if (index != null)
                        {
                            Operations++;
                            index.Remove(Int64.Parse(split[1]));
                            PrintInfo();
                        }
                        break;
                    case "G":
                        if (index != null)
                        {
                            var rec = index.Get(Int64.Parse(split[1]));
                            if(rec != null)
                                Console.WriteLine(rec);
                            PrintInfo();
                        }
                        break;
                    case "RO":
                        if (index != null)
                        {
                            index.Reorganize();
                            PrintInfo();
                        }
                        break;
                    case "PI":
                        if (index != null)
                            index.PrintIndex();
                        break;
                    case "PF":
                        if (index != null)
                            index.PrintMainFile();
                        break;
                    case "PA":
                        if (index != null)
                            index.PrintAllMainFile();
                        break;
                    case "S":
                        PrintInfo();
                        break;
                    case "TEST":
                        if (index != null)
                        {
                            int amount = Int32.Parse(split[1]);
                            List<long> keyList = Randoms.RandomKeys(amount, 1, 10000);
                                //Randoms.GenerateRandom(amount, 1, 1000);
                            foreach (long i in keyList)
                            {
                                Tuple<long, long, long> coeffs = Randoms.GenerateCoefficients();
                                index.Add(new Record(i, coeffs.Item1, coeffs.Item2, coeffs.Item3));
                                Operations++;
                            }
                            PrintInfo();
                        }
                        break;
                    default:
                        Console.WriteLine("Bad command, try again!");
                        break;
                }
            }
            if (index != null)
                index.Dispose();
        }

        private static void PrintInfo()
        {
            Console.WriteLine("-------------------");
            Console.WriteLine("Operations: " + Operations);
            Console.WriteLine("Reads: " + MainReads);
            Console.WriteLine("Writes: " + MainWrites);
            Console.WriteLine("Pages PA: " + Index.MainPages);
            Console.WriteLine("Pages OA: " + Index.OverflowPages);
            Console.WriteLine("-------------------");
        }
    }
}
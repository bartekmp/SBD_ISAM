using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/*
 * F mainFile index mainPageSize indexPageSize - read index from "index" file
 * N mainFile index mainPageSize indexPageSize - create new index in "index" file, with given page size
 * A key [A B C] - add record with given key, optional coefficients
 * U key A B C - update record value
 * R key - remove record with given key
 * PI - print BTree
 * PF - print main file in order
 */

namespace ISAM
{
    public static class Program
    {
        public static Random random = new Random();
        public static long IndexReads = 0L, IndexWrites = 0L, MainReads = 0L, MainWrites = 0L;
        public static long Reads { get { return IndexReads + MainReads; } }
        public static long Writes { get { return IndexWrites + MainWrites; } }

        static void Main(string[] args)
        {
            Index index = null;
            string readLine;
            while ((readLine = Console.ReadLine()) != null)
            {
                var split = readLine.Split(new[] { ' ' });
                switch (split[0])
                {
                    case "F":
                        if (index == null)
                            index = new Index(split[1], split[2], Int32.Parse(split[3]), Int32.Parse(split[4]), Index.Mode.Read);
                        break;
                    case "N":
                        if (index == null)
                            index = new Index(split[1], split[2], Int32.Parse(split[3]), Int32.Parse(split[4]), Index.Mode.New);
                        break;
                    case "A":
                        if (index != null)
                        {
                            if(split.Length == 4)
                            index.Add(new Record(Int64.Parse(split[1]), Int64.Parse(split[2]), Int64.Parse(split[3]),
                                Int64.Parse(split[4])));
                            if(split.Length == 1)
                                index.
                        }
                        break;
                    case "U":
                        if (index != null)
                            index.Update(new Record(Int64.Parse(split[1]), Int64.Parse(split[2]), Int64.Parse(split[3]), Int64.Parse(split[4])));
                        break;
                    case "R":
                        if (index != null)
                            index.Remove(Int64.Parse(split[1]));
                        break;
                    case "RO":
                        if (index != null)
                            index.Reorganize();
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
                        //Console.WriteLine("Reads: " + Reads);
                        //Console.WriteLine("Writes: " + Writes);
                        //Console.WriteLine("Index Reads: " + IndexReads);
                        //Console.WriteLine("Index Writes: " + IndexWrites);
                        Console.WriteLine("-------------------");
                        Console.WriteLine("Main Reads: " + MainReads);
                        Console.WriteLine("Main Writes: " + MainWrites);
                        Console.WriteLine("-------------------");
                        break;
                    default:
                        Console.WriteLine("Bad command, try again!");
                        break;
                }
            }
            if (index != null)
                index.Dispose();

        }

        public static Record RandomRecord()
        {

        }
    }
}

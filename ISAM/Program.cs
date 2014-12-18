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
                            index = new Index(split[1], split[2], int.Parse(split[3]), int.Parse(split[4]), Index.Mode.Read);
                        break;
                    case "N":
                        if (index == null)
                            index = new Index(split[1], split[2], int.Parse(split[3]),int.Parse(split[4]), Index.Mode.New);
                        break;
                    case "A":
                        if (index != null)
                            index.Add(new Record(long.Parse(split[1]), long.Parse(split[2]), long.Parse(split[3]), long.Parse(split[4])));
                        break;
                    case "U":
                        if (index != null)
                            index.Update(new Record(long.Parse(split[1]), long.Parse(split[2]), long.Parse(split[3]), long.Parse(split[4])));
                        break;
                    case "R":
                        if (index != null)
                            index.Remove(long.Parse(split[1]));
                        break;
                    case "RO":
                        if(index != null)
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
                    default:
                        Console.WriteLine("Bad command, try again!");
                        break;
                }
            }

        }
    }
}

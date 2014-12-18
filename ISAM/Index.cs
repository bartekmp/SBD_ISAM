using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ISAM
{
    public class Index
    {
        public IndexReader IndexReader;
        public IndexWriter IndexWriter;
        public MainReader MainReader;
        public MainWriter MainWriter;

        public static long Pages = 0L, OverflowAddress = -1L, OverflowFirstPageNumber = -1L, OverflowCount = 0L, OverflowEndAddress = -1L, OverflowPagesCount = 0L;

        public int IndexPageSize, MainPageSize;
        public double Alpha;
        private int _pageSizeInBytes;
        private string _indexPath, _filePath;

        private const long NoOverflow = -1L;
        private const long EmptyPagesLimit = 2;
        private const long OverflowPagesInitialCount = EmptyPagesLimit / 2;
        public enum Mode
        {
            Read, New
        }

        public Index(string fileName, string indexName, int mainPageSize, int indexPageSize, Mode mode = Mode.Read)
        {
            IndexPageSize = indexPageSize;
            MainPageSize = mainPageSize;
            _indexPath = indexName;
            _filePath = fileName;
            Alpha = 4 / 7;
            IndexReader = new IndexReader(indexName, IndexPageSize, mode);
            IndexWriter = new IndexWriter(indexName, IndexPageSize, mode);
            MainReader = new MainReader(fileName, MainPageSize, mode);
            MainWriter = new MainWriter(fileName, MainPageSize, mode);

            IndexPage.PageSize = indexPageSize;
            FilePage.PageSize = mainPageSize;

            if (mode == Mode.New)
                Initialize();
        }

        private void Initialize()
        {
            //todo initialize index with watcher
            //fill primary area with empty pages
            //set overflow area address
            long addrI = 0L, addrM = 0L;

            var firstIndexPage = new IndexPage { Address = addrI++, Count = 1 };
            firstIndexPage.Entries[0] = new Tuple<long, long>(0, 0);
            IndexWriter.WritePage(firstIndexPage);

            var firstMainPage = new FilePage { Address = addrM++, Count = 1 };
            firstMainPage.Entries[0] = new Tuple<Record, long>(new Record(), NoOverflow);
            MainWriter.WritePage(firstMainPage);

            for (; addrI < EmptyPagesLimit; addrI++)
            {
                var newPage = new IndexPage { Address = addrI };
                IndexWriter.WritePage(newPage);
            }

            for (; addrM < EmptyPagesLimit; addrM++)
            {
                var newPage = new FilePage { Address = addrM };
                MainWriter.WritePage(newPage);
            }

            OverflowAddress = MainWriter.Writer.Position;
            OverflowFirstPageNumber = MainReader.PageNumberFromAddress(MainWriter.Writer.Position);

            for (; addrM < EmptyPagesLimit + OverflowPagesInitialCount; addrM++) // overflow pre-alloc
            {
                var newPage = new FilePage { Address = addrM };
                MainWriter.WritePage(newPage);

            }

            OverflowEndAddress = MainWriter.Writer.Position;

            MainWriter.Reset();
            IndexWriter.Reset();

            Program.IndexWrites = 0L;
            Program.MainWrites = 0L;

        }

        /// <summary>
        /// finds an entry in index which is lesser or equal key
        /// </summary>
        /// <param name="key"></param>
        /// <returns>a tuple of bool, long, long and long - success, index key, file page address and place of index key</returns>
        private Tuple<bool, long, long, long> FindInIndex(long key)
        {
            var cntr = 0;
            var found = false;
            while (!found)
            {
                var page = IndexReader.ReadPage(cntr++);
                if (page == null)
                    break;
                for (int i = (int)page.Count - 1; i >= 0; i--)
                {
                    if (page.Entries[i].Item1 <= key)
                    {
                        found = true;
                        return new Tuple<bool, long, long, long>(true, page.Entries[i].Item1, page.Entries[i].Item2, cntr - 1);
                    }

                }
            }
            return new Tuple<bool, long, long, long>(false, -1, -1, -1);
        }

        // look for a key on given page
        public Tuple<bool, long, long> FindInFilePage(long page, long key)
        {
            var filePage = MainReader.ReadPage(page);
            long tmp = 0;
            bool flag = false;
            for (int i = filePage.Entries.Count; i >= 0; i--)
            {
                if (filePage.Entries[i].Item1.Key == key)
                {
                    return new Tuple<bool, long, long>(true, filePage.Address, i);
                }
                if (filePage.Entries[i].Item1.Key < key && filePage.Entries[i].Item2 != -1)
                {
                    tmp = filePage.Entries[i].Item2;
                    flag = true;
                    break;
                }
            }
            if (flag)
            {
                while (tmp != -1)
                {
                    var rec = MainReader.ReadEntry(tmp);
                    if (rec.Item1.Key == key)
                    {
                        return new Tuple<bool, long, long>(true, MainReader.LastPage.Address, MainReader.LastRecordNumber);
                    }
                    if (rec.Item1.Key < key && rec.Item2 != -1)
                    {
                        tmp = rec.Item2;
                    }
                }
            }
            return new Tuple<bool, long, long>(false, -1, -1);
        }

        // find whether there's already a record with given key in file
        public Tuple<bool, long, long> FindKey(long key)
        {
            var address = FindInIndex(key); // try to find on which page could possibly be that record
            if (address == null || address.Item1 == false)
                return new Tuple<bool, long, long>(false, -1, -1);

            var filePage = MainReader.ReadPage(address.Item2);
            long link = 0;
            bool overflowFlag = false; // if record has a link to OA

            for (int i = filePage.Entries.Count - 1; i >= 0; i--)
            {
                if (filePage.Entries[i].Item1.Key == key) // if record is found
                {
                    return new Tuple<bool, long, long>(true, filePage.Address, i);
                }
                if (filePage.Entries[i].Item1.Key < key && filePage.Entries[i].Item2 != -1) // if some record has overflow pointer which can possibly be given record
                {
                    link = filePage.Entries[i].Item2; // link to overflow
                    overflowFlag = true;
                    break;
                }
            }

            if (overflowFlag)
            {
                while (link != -1) // while there is still next element in overflow chain
                {
                    var rec = MainReader.ReadEntry(link);
                    if (rec.Item1.Key == key)
                    {
                        return new Tuple<bool, long, long>(true, MainReader.LastPage.Address, MainReader.LastRecordNumber);
                    }
                    if (rec.Item1.Key < key && rec.Item2 != -1) // go deeper
                    {
                        link = rec.Item2;
                    }
                }
            }
            return new Tuple<bool, long, long>(false, -1, -1); // not found
        }

        //todo
        public void Add(Record r)
        {
            var key = r.Key;
            var found = FindKey(key);
            if (found.Item1)
            {
                Console.WriteLine("Key already added");
                return;
            }

            //look for a place in available pages in primary area
            //when there's no place, try to add in overflow area
            //when unable to do that, add to new page and to index
            var index = FindInIndex(key);
            var indexPage = index.Item4;
            if (index.Item1 == false)
            {
                Console.WriteLine("No place");
            }
            var newPage = MainReader.ReadPage(index.Item3);
            //add on current page
            if (newPage.Count < MainPageSize)
            {
                newPage.Entries[(int)newPage.Count] = new Tuple<Record, long>(r, -1);
                newPage.Count++;
                Sort(newPage);

                MainWriter.WritePage(newPage);
            }
            else
            {
                //add to overflow and link
                if (key < newPage.Entries.Last().Item1.Key)
                {
                    //todo
                    //find a record to which link new record from overflow
                    int linkedRecordNumber = newPage.Entries.Count - 1;
                    long linkAddress = 0;

                    for (; linkedRecordNumber >= 0; linkedRecordNumber--)
                        if (newPage.Entries[linkedRecordNumber].Item1.Key < key)
                            break;

                    MainReader.Reader.Position = OverflowAddress;
                    var overflowPage = MainReader.ReadNextPage();
                    if (overflowPage.Count < MainPageSize)
                    {
                        overflowPage.Entries[(int)overflowPage.Count] = new Tuple<Record, long>(r, -1);
                        linkAddress = overflowPage.Address*MainPageSize + overflowPage.Count;
                        overflowPage.Count++;
                        Sort(overflowPage);
                        MainWriter.WritePage(overflowPage);
                    }
                    newPage.Entries[linkedRecordNumber] = new Tuple<Record, long>(newPage.Entries[linkedRecordNumber].Item1, linkAddress);
                    MainWriter.WritePage(newPage);

                }
                else //add to new page
                {
                    newPage = MainReader.ReadPage(index.Item3 + 1);
                    if (newPage.Count < MainPageSize)
                    {
                        newPage.Entries[(int)newPage.Count] = new Tuple<Record, long>(r, -1);
                        newPage.Count++;
                        Sort(newPage);
                        MainWriter.WritePage(newPage);
                    }

                    var newIndexPage = IndexReader.ReadPage(indexPage);
                    //todo add to index
                }
            }
        }

        public void Remove(Record r)
        {
            Remove(r.Key);
        }

        public void Remove(long key)
        {
            if (key <= 0)
            {
                Console.WriteLine("Cannot remove that record");
                return;
            }
            var found = FindKey(key);
            if (!found.Item1)
                Console.WriteLine("No such record");
            var page = MainReader.ReadPage(found.Item2);
            for (int i = 0; i < page.Count; i++)
            {
                if (page.Entries[i].Item1.Key == key)
                {
                    page.Entries[i].Item1.Deleted = true;
                    MainWriter.WritePage(page);
                    break;
                }
            }
        }

        public void Update(Record r)
        {
            var found = FindKey(r.Key);
            if (!found.Item1)
                Console.WriteLine("No such record");
            var page = MainReader.ReadPage(found.Item2);
            page.Entries[(int)found.Item3].Item1.A = r.A;
            page.Entries[(int)found.Item3].Item1.B = r.B;
            page.Entries[(int)found.Item3].Item1.C = r.C;
            MainWriter.WritePage(page);
        }

        private string EntryToString(Tuple<Record, long> entry)
        {
            var sb = new StringBuilder();
            if (entry.Item1.Key == 0)
            {
                sb.Append("STRAZNIK");
            }
            else
            {
                sb.Append((entry.Item1.Key == long.MaxValue ? "-" : entry.Item1.ToString()) + " | " +
                          (entry.Item2 == -1 ? "-" : entry.Item2.ToString()) + (entry.Item1.Deleted ? " [X]" : ""));

            }
            return sb.ToString();
        }

        public void PrintIndex()
        {
            using (var ir = new IndexReader(_indexPath, IndexPageSize, Mode.Read, false))
            {
                Console.WriteLine("Entries per page: " + IndexPageSize);
                var entry = 0L;
                Tuple<long, long> i;
                while ((i = ir.ReadEntry(entry++)) != null)
                {
                    if (ir.LastPage.Count <= 0)
                        break;

                    string output = "";
                    output += "" + (i.Item1 == long.MaxValue ? "-" : i.Item1.ToString()) + " | " + (i.Item2 == -1 ? "-" : i.Item2.ToString());
                    Console.WriteLine(output);
                }
            }
        }

        public void PrintMainFile()
        {
            using (var mr = new MainReader(_filePath, MainPageSize, Mode.Read, false))
            {
                Console.WriteLine("Entries per page: " + MainPageSize);
                var entry = 0L;
                Tuple<Record, long> i;
                while ((i = mr.ReadEntry(entry)) != null)
                {
                    if (mr.LastPage.Count <= 0 || mr.LastPage.Address >= OverflowAddress)
                        break;
                    Console.WriteLine(EntryToString(i));

                    if (i.Item2 != -1)
                        FollowChain(i.Item2);

                    entry++;
                }
            }
        }

        private void FollowChain(long address)
        {
            var tmp = address;
            string spacing = " ";
            while (tmp != -1)
            {
                var i = MainReader.ReadEntry(tmp);
                Console.WriteLine(spacing + (i.Item1.Key == long.MaxValue ? "-" : i.Item1 + " | " +
                              (i.Item2 == -1 ? "-" : i.Item2.ToString())));
                spacing += " ";
                tmp = i.Item2;
            }
        }

        //todo
        public void Reorganize()
        {

        }



        public void Sort(FilePage fp)
        {
            fp.Entries.Sort((x, y) => x.Item1.Key.CompareTo(y.Item1.Key));
        }
    }



    public class FilePage
    {
        //main file page
        // |       cnt        |
        // | record | pointer |
        // | record | pointer |
        // |    .........     |

        public static int PageSize; // in records
        public static int PageSizeInBytes { get { return PageSize * 17 + 8; } }
        public List<Tuple<Record, long>> Entries;
        public long Count = 0L;
        public long Address = -1L;

        public FilePage()
        {
            Entries = new List<Tuple<Record, long>>(new Tuple<Record, long>[PageSize]);
            for (int i = 0; i < PageSize; ++i)
            {
                Entries[i] = new Tuple<Record, long>(Record.EmptyRecord(), -1L);
            }

        }

        public static FilePage EmptyPage()
        {
            return new FilePage();
        }

    }

    public class IndexPage
    {
        //index page
        // |     cnt       |
        // | key | pointer |
        // | key | pointer |
        // |     ......    |
        public static int PageSize;
        public static int PageSizeInBytes { get { return PageSize * 16 + 8; } }

        public List<Tuple<long, long>> Entries;
        public long Count = 0L;
        public long Address = -1L;

        public IndexPage()
        {
            Entries = new List<Tuple<long, long>>(new Tuple<long, long>[PageSize]);
            for (int i = 0; i < PageSize; ++i)
            {
                Entries[i] = new Tuple<long, long>(long.MaxValue, -1L);
            }
        }
        public static IndexPage EmptyPage()
        {
            return new IndexPage();
        }

    }
}

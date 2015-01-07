using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ISAM
{
    public class Index
    {
        public enum Mode
        {
            Read,
            New
        }

        public static long MainPages = 0L,
            OverflowAddress = -1L,
            OverflowFirstPageNumber = -1L,
            OverflowPages = 0L,
            OverflowEndAddress = -1L,
            OverflowRecordCount = 0L,
            MainRecordCount = 0L;

        public static long TempLong = -1;
        public static int MainPageSize;
        public static double Alpha = 4 / (double)7;
        private readonly string _filePath;
        public MainReader MainReader;
        public MainWriter MainWriter;
        private string _indexPath;
        private int _pageSizeInBytes;


        public Index(string fileName, string indexName, int mainPageSize = 0, Mode mode = Mode.Read)
        {
            MainPageSize = mainPageSize;
            _indexPath = indexName;
            _filePath = fileName;

            IndexUnit.Path = indexName;

            MainReader = new MainReader(fileName, MainPageSize, mode);
            MainWriter = new MainWriter(fileName, MainPageSize, mode);

            FilePage.PageSize = mainPageSize;

            if (mode == Mode.New)
                Initialize();
            if (mode == Mode.Read)
            {
                MetaData.Read();
                IndexUnit.Init(false);
                IndexUnit.ReadIndex();
                FilePage.PageSize = MainPageSize;
                MainReader = new MainReader(fileName, MainPageSize, mode);
                MainWriter = new MainWriter(fileName, MainPageSize, mode);
                IndexUnit.WriteIndex();
            }
            MetaData.Save();
        }

        private void ResetVariables()
        {
            MainPages = 0;
            OverflowAddress = -1;
            OverflowPages = 0;
            OverflowRecordCount = 0;
            MainRecordCount = 0;
        }

        private void Initialize()
        {
            //fill primary area with empty pages
            //set overflow area address
            long addrI = 0L, addrM = 0L;

            try
            {
                File.Delete("meta");
            }
            catch
            {
            }

            IndexUnit.Init(true);
            var firstMainPage = new FilePage { Address = addrM++, Count = 1 };
            firstMainPage.Entries[0] = new Tuple<Record, long>(new Record(), -1);
            MainWriter.WritePage(firstMainPage);
            MainPages = 1;
            MainRecordCount = 1;

            OverflowAddress = MainWriter.Writer.Position;
            OverflowFirstPageNumber = MainReader.PageNumberFromAddress(MainWriter.Writer.Position);

            var newPage = new FilePage { Address = addrM };
            MainWriter.WritePage(newPage);

            OverflowEndAddress = MainWriter.Writer.Position;
            OverflowPages = 1;
            MainWriter.Reset();

            Program.MainWrites = 0L;
            MetaData.Save();
            IndexUnit.WriteIndex();
        }

        /// <summary>
        ///     finds an entry in index which is lesser or equal key
        /// </summary>
        /// <param name="key"></param>
        /// <returns>a tuple of bool, long, long and long - success, index key, file page address and place of index key</returns>
        private Tuple<bool, long, long, long> FindInIndex(long key)
        {
            int cntr = 0;
            bool found = false;
            //while (!found)
            {
                List<Tuple<long, long>> page = IndexUnit.Entries; //IndexReader.ReadPage(cntr++);
                //if (page == null)
                //    break;
                for (int i = page.Count - 1; i >= 0; i--)
                {
                    if (page[i].Item1 <= key)
                    {
                        found = true;
                        return new Tuple<bool, long, long, long>(true, page[i].Item1, page[i].Item2, cntr - 1);
                    }
                }
            }
            return new Tuple<bool, long, long, long>(false, -1, -1, -1);
        }

        // look for a key on given page
        public Tuple<bool, long, long> FindInFilePage(long page, long key)
        {
            FilePage filePage = MainReader.ReadPage(page);
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
                    Tuple<Record, long> rec = MainReader.ReadEntry(tmp);
                    if (rec.Item1.Key == key)
                    {
                        return new Tuple<bool, long, long>(true, MainReader.LastPage.Address,
                            MainReader.LastRecordNumber);
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
            Tuple<bool, long, long, long> address = FindInIndex(key);
            // try to find on which page could possibly be that record
            if (address == null || address.Item1 == false)
                return new Tuple<bool, long, long>(false, -1, -1);

            long pageAddress = address.Item3;
            FilePage filePage = null;
            while ((filePage = MainReader.ReadPage(pageAddress++)) != null)
            {
                for (int i = filePage.Entries.Count - 1; i >= 0; i--)
                {
                    if (filePage.Entries[i].Item1.Key == key) // if record is found
                    {
                        return new Tuple<bool, long, long>(true, filePage.Address, i);
                    }
                    if (filePage.Entries[i].Item1.Key < key && filePage.Entries[i].Item2 != -1)
                    // if some record has overflow pointer which can possibly be given record
                    {
                        Tuple<bool, long, long> search = FindKeyInOverflowChain(filePage.Entries[i].Item2, key);
                        // look for the key in overflow chain
                        if (search.Item1)
                            return search;
                    }
                }
            }
            return new Tuple<bool, long, long>(false, -1, -1); // not found
        }

        private Tuple<bool, long, long> FindKeyInOverflowChain(long entry, long key)
        {
            long link = entry;
            while (link != -1) // while there is still next element in overflow chain
            {
                Tuple<Record, long> rec = MainReader.ReadEntry(link);
                if (rec.Item1.Key == key)
                {
                    return new Tuple<bool, long, long>(true, MainReader.LastPage.Address,
                        MainReader.LastRecordNumber);
                }
                if (rec.Item1.Key < key && rec.Item2 != -1) // go deeper
                {
                    link = rec.Item2;
                }
                else
                {
                    return new Tuple<bool, long, long>(false, -1, -1); // not found
                }
            }
            return new Tuple<bool, long, long>(false, -1, -1); // not found
        }

        public Record Get(long key)
        {
            var found = FindKey(key);
            if (!found.Item1)
            {
                Console.WriteLine("Not found");
                return null;
            }
            var page = MainReader.ReadPage(found.Item2);
            if (page == null)
            {
                Console.WriteLine("Not found");
                return null;
            }
            return page.Entries[(int)found.Item3].Item1;

        }
        public void Add(Record r)
        {
            long key = r.Key;
            Tuple<bool, long, long> found = FindKey(key);
            if (found.Item1)
            {
                Console.WriteLine("Key already added");
                return;
            }

            //look for a place in available pages in primary area
            //when there's no place, try to add in overflow area
            //when unable to do that, add to new page and to index
            Tuple<bool, long, long, long> index = FindInIndex(key);
            long indexPage = index.Item4;
            if (index.Item1 == false)
            {
                Console.WriteLine("No place");
            }
            FilePage newPage = MainReader.ReadPage(index.Item3);
            //add on current page
            if (newPage.Count < MainPageSize)
            {
                newPage.Entries[(int)newPage.Count] = new Tuple<Record, long>(r, -1);
                newPage.Count++;
                Sort(newPage);
                MainRecordCount++;
                MainWriter.WritePage(newPage);
                MetaData.Save();
            }
            else
            {
                //find a record to which link new record from overflow
                int linkedRecordNumber = newPage.Entries.Count - 1;
                long linkAddress = 0;
                FilePage mainPageWithLink = newPage;

                for (; linkedRecordNumber >= 0; linkedRecordNumber--)
                    if (mainPageWithLink.Entries[linkedRecordNumber].Item1.Key < key)
                        break;

                if (mainPageWithLink.Entries[linkedRecordNumber].Item2 != -1)
                // if a record in primary area already has pointer to overflow
                {
                    while (true)
                    {
                        long id = mainPageWithLink.Entries[linkedRecordNumber].Item2;
                        if (id == -1)
                            break;
                        Tuple<Record, long> newEntry = MainReader.ReadEntry(id);
                        if (key > newEntry.Item1.Key)
                        {
                            mainPageWithLink = MainReader.LastPage;
                            for (int i = 0; i < mainPageWithLink.Count; i++)
                                if (mainPageWithLink.Entries[i].Item1.Key == newEntry.Item1.Key)
                                    linkedRecordNumber = i;
                        }
                        else
                        {
                            break;
                        }
                    }
                }

                MainReader.Reader.Position = OverflowAddress;
                FilePage overflowPage = MainReader.ReadNextPage(); // read next page from overflow area

                while (overflowPage != null && overflowPage.Count >= MainPageSize)
                    // find a page with enough space to place a new record
                    overflowPage = MainReader.ReadNextPage();

                if (overflowPage == null)
                {
                    AllocateEmptyPageAtTheEnd(ref MainWriter); // allocate new page for overflow area
                    overflowPage = MainReader.ReadNextPage();
                }
                if (overflowPage.Address == mainPageWithLink.Address)
                {
                    overflowPage = mainPageWithLink;
                }

                if (overflowPage.Count < MainPageSize)
                {
                    overflowPage.Entries[(int)overflowPage.Count] = new Tuple<Record, long>(r,
                        mainPageWithLink.Entries[linkedRecordNumber].Item2);
                    linkAddress = overflowPage.Address * MainPageSize + overflowPage.Count;
                    overflowPage.Count++;
                    OverflowRecordCount++;
                    mainPageWithLink.Entries[linkedRecordNumber] =
                        new Tuple<Record, long>(mainPageWithLink.Entries[linkedRecordNumber].Item1, linkAddress);
                    MainWriter.WritePage(overflowPage);
                    MetaData.Save();
                }
                if (overflowPage.Address != mainPageWithLink.Address)
                    MainWriter.WritePage(mainPageWithLink);
                MetaData.Save();
            }
            if (OverflowRecordCount > 0.5 * MainRecordCount)
                Reorganize();
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
            Tuple<bool, long, long> found = FindKey(key);
            if (!found.Item1)
            {
                Console.WriteLine("No such record");
                return;
            }
            FilePage page = MainReader.ReadPage(found.Item2);
            for (int i = 0; i < page.Count; i++)
            {
                if (page.Entries[i].Item1.Key == key)
                {
                    page.Entries[i].Item1.Deleted = true;
                    MainWriter.WritePage(page);
                    MetaData.Save();
                    break;
                }
            }
        }

        public void Update(Record r)
        {
            Tuple<bool, long, long> found = FindKey(r.Key);
            if (!found.Item1)
            {
                Console.WriteLine("No such record");
                return;
            }
            FilePage page = MainReader.ReadPage(found.Item2);
            page.Entries[(int)found.Item3].Item1.A = r.A;
            page.Entries[(int)found.Item3].Item1.B = r.B;
            page.Entries[(int)found.Item3].Item1.C = r.C;
            MainWriter.WritePage(page);
            MetaData.Save();
        }

        private string EntryToString(Tuple<Record, long> entry)
        {
            var sb = new StringBuilder();
            if (entry.Item1.Key == 0)
            {
                sb.Append(entry.Item1.Key + " | " + (entry.Item2 == -1 ? "-" : "#" + entry.Item2));
            }
            else
            {
                sb.Append((entry.Item1.Key == long.MaxValue ? "-" : entry.Item1.ToString()) + " | " +
                          (entry.Item2 == -1 ? "-" : "#" + entry.Item2) + (entry.Item1.Deleted ? " [X]" : ""));
            }
            return sb.ToString();
        }

        public void PrintIndex()
        {
            foreach (var entry in IndexUnit.Entries)
            {
                Console.WriteLine(entry.Item1 + " | " + entry.Item2);
            }
        }

        public void PrintMainFile()
        {
            var mr = new MainReader(_filePath, MainPageSize, Mode.Read, false);
            {
                Console.WriteLine("Entries per page: " + MainPageSize);
                long entry = 0L;
                long countRecs = 0;
                Tuple<Record, long> i;
                while ((i = mr.ReadEntry(entry)) != null)
                {
                    if (mr.LastPage.Count <= 0)
                        break;
                    if (mr.LastPage.Address >= MainReader.PageNumberFromAddress(OverflowAddress))
                        break;
                    if (countRecs >= MainPageSize)
                    {
                        countRecs = 1;
                        Console.WriteLine();
                    }
                    else
                    {
                        countRecs++;
                    }
                    Console.WriteLine(EntryToString(i));

                    if (i.Item2 != -1)
                        FollowChain(i.Item2, ref mr);

                    entry++;
                }
            }
            mr.Dispose();
        }

        private void FollowChain(long address, ref MainReader mr)
        {
            long tmp = address;
            string spacing = " ";
            while (tmp != -1)
            {
                Tuple<Record, long> i = mr.ReadEntry(tmp);
                Console.WriteLine(spacing + (i.Item1.Key == long.MaxValue
                    ? "-"
                    : i.Item1 + " | " +
                      (i.Item2 == -1 ? "-" : "#" + i.Item2)) + (i.Item1.Deleted ? " [X]" : ""));
                spacing += " ";
                tmp = i.Item2;
            }
        }

        public void PrintAllMainFile()
        {
            using (var mr = new MainReader(_filePath, MainPageSize, Mode.Read, false))
            {
                Console.WriteLine("Entries per page: " + MainPageSize);
                long entry = 0L;
                Tuple<Record, long> i;
                bool flag = true;
                while ((i = mr.ReadEntry(entry)) != null)
                {
                    if (mr.LastPage.Address >= MainReader.PageNumberFromAddress(OverflowAddress) && flag)
                    {
                        flag = false;
                        Console.WriteLine("\n###OVERFLOW###\n");
                    }
                    Console.WriteLine(EntryToString(i));
                    entry++;
                }
            }
        }


        //todo
        public void Reorganize()
        {
            Console.WriteLine("###REORGANIZATION###");
            var NewMainReader = new MainReader("newmain", MainPageSize, Mode.New);
            var NewMainWriter = new MainWriter("newmain", MainPageSize, Mode.New);
            TempLong = OverflowAddress;
            ResetVariables();
            AllocateEmptyPageAtTheEnd(ref NewMainWriter);
            MainPages++;
            FilePage newPage = NewMainReader.ReadNextPage();
            Tuple<Record, long> entry;
            MainReader.Reader.Position = 0;
            //MainReader._count = false;

            while ((entry = MainReader.ReadNextEntryWithChaining()) != null)
            {
                if (entry.Item1.Key == long.MaxValue)
                    continue;
                if (entry.Item1.Deleted)
                    continue;
                if (newPage.Count >= Alpha * MainPageSize)
                {
                    // insert new element to index
                    // allocate new page

                    NewMainWriter.WritePage(newPage);
                    AllocateEmptyPageAtTheEnd(ref NewMainWriter);
                    newPage = NewMainReader.ReadNextPage();
                    MainPages++;
                    if (entry.Item1.Key != long.MaxValue)
                        IndexUnit.Entries.Add(new Tuple<long, long>(entry.Item1.Key, MainPages - 1));
                }

                newPage.Entries[(int)newPage.Count++] = new Tuple<Record, long>(entry.Item1, -1);
                MainRecordCount++;
            }
            NewMainWriter.WritePage(newPage);
            OverflowAddress = NewMainWriter.Writer.Position;
            AllocateEmptyPageAtTheEnd(ref NewMainWriter); // allocate overflow
            OverflowPages++;
            IndexUnit.Sort();
            IndexUnit.WriteIndex();
            MetaData.Save();
            MainReader.Dispose();
            MainWriter.Dispose();
            NewMainReader.Dispose();
            NewMainWriter.Dispose();
            File.Delete(_filePath);
            File.Move("newmain", _filePath);
            MainReader = new MainReader(_filePath, MainPageSize, Mode.Read);
            MainWriter = new MainWriter(_filePath, MainPageSize, Mode.Read);
        }

        private void AllocateEmptyPageAtTheEnd(ref MainWriter mw)
        {
            mw.Writer.Position = mw.Writer.Length;
            var page = new FilePage { Address = MainReader.PageNumberFromAddress(mw.Writer.Position) };
            mw.WritePage(page);
        }

        private void Sort(FilePage fp)
        {
            fp.Entries.Sort((x, y) => x.Item1.Key.CompareTo(y.Item1.Key));
        }

        public void Dispose()
        {
            MainReader.Dispose();
            MainWriter.Dispose();
            MetaData.Dispose();
            IndexUnit.Dispose();
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

        public long Address = -1L;
        public long Count = 0L;
        public List<Tuple<Record, long>> Entries;

        public FilePage()
        {
            Entries = new List<Tuple<Record, long>>(new Tuple<Record, long>[PageSize]);
            for (int i = 0; i < PageSize; ++i)
            {
                Entries[i] = new Tuple<Record, long>(Record.EmptyRecord(), -1L);
            }
        }

        public static int PageSizeInBytes
        {
            get { return PageSize * 17 + 8; }
        }

        public static FilePage EmptyPage()
        {
            return new FilePage();
        }
    }

    public static class MetaData
    {
        // FilePagesCount - long
        // IndexPagesCount - long
        // RecordCount - long
        // OverflowAddress - long
        // OverflowPagesCount -long
        // OverflowRecordCount - long
        // MainPageSize - int
        // IndexPageSize - int

        private static readonly BinaryReader Reader =
            new BinaryReader(new FileStream("meta", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));

        private static readonly BinaryWriter Writer =
            new BinaryWriter(new FileStream("meta", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));

        public static void Save()
        {
            Writer.BaseStream.Position = 0;
            Writer.Write(Index.MainPages);
            Writer.Write(Index.MainRecordCount);
            Writer.Write(Index.OverflowAddress);
            Writer.Write(Index.OverflowPages);
            Writer.Write(Index.OverflowRecordCount);
            Writer.Write(Index.Alpha);
            Writer.Write(Index.MainPageSize);
            Writer.Flush();
        }

        public static void Read()
        {
            Reader.BaseStream.Position = 0;
            Index.MainPages = Reader.ReadInt64();
            Index.MainRecordCount = Reader.ReadInt64();
            Index.OverflowAddress = Reader.ReadInt64();
            Index.OverflowFirstPageNumber = MainReader.PageNumberFromAddress(Index.OverflowAddress);
            Index.OverflowPages = Reader.ReadInt64();
            Index.OverflowRecordCount = Reader.ReadInt64();
            Index.Alpha = Reader.ReadDouble();
            Index.MainPageSize = Reader.ReadInt32();
        }

        public static void Dispose()
        {
            Reader.Dispose();
            Writer.Dispose();
        }
    }

    public static class IndexUnit
    {
        public static List<Tuple<long, long>> Entries;
        public static int Count;
        private static BinaryReader Reader;
        private static BinaryWriter Writer;
        public static string Path = "index";

        public static void Init(bool withWatcher)
        {
            Reader =
                new BinaryReader(new FileStream(Path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
            Writer =
                new BinaryWriter(new FileStream(Path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
            Entries = new List<Tuple<long, long>>();
            if (withWatcher)
            {
                Count = 1;
                Entries.Add(new Tuple<long, long>(0, 0));
            }
        }

        public static void Sort()
        {
            Entries.Sort((x, y) => x.Item1.CompareTo(y.Item1));
        }

        public static void ReadIndex()
        {
            Reader.BaseStream.Position = 0;
            int cnt = Reader.ReadInt32();
            Count = cnt;
            while (cnt-- > 0)
            {
                long key = Reader.ReadInt64();
                long ptr = Reader.ReadInt64();
                var tuple = new Tuple<long, long>(key, ptr);
                Entries.Add(tuple);
            }
            Sort();
        }

        public static void WriteIndex()
        {
            Writer.BaseStream.Position = 0;
            Writer.Write(Count);
            foreach (var entry in Entries)
            {
                Writer.Write(entry.Item1);
                Writer.Write(entry.Item2);
            }
            Writer.Flush();
        }

        public static void Dispose()
        {
            Reader.Dispose();
            Writer.Dispose();
        }
    }
}
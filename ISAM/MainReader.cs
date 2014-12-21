using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ISAM
{
    public class MainReader : IDisposable
    {
        public BufferedStream Reader;
        public FilePage LastPage;
        public long LastPageNumber = -1L;
        public long LastRecordNumber = -1L;
        public long NextRecordNumber = -1L;
        private string _path;
        private static int _pageSize;
        private bool _eof = false, _count = true;
        private long _counter = 0;
        private static int _pageSizeInBytes { get { return _pageSize * 41 + 8;  } }
        public static long PageByteAddress(long page)
        {
            return page * (_pageSize * 41 + 8);
        }

        public static long PageNumberFromAddress(long address)
        {
            return address/(_pageSize*41 + 8);
        }

        public MainReader(string path, int pageSize, Index.Mode m, bool count = true)
        {
            _path = path;
            _pageSize = pageSize;

            var mode = m == Index.Mode.Read ? FileMode.Open : FileMode.Create;
            Reader = new BufferedStream(new FileStream(_path, mode, FileAccess.ReadWrite, FileShare.ReadWrite),
                _pageSizeInBytes);
            _count = count;

        }

        public FilePage ReadPage(long page)
        {
            if (_eof)
            {
                return LastPage;
            }
            if (page == LastPageNumber)
                return LastPage;

            //Reader.Position = PageByteAddress(page);

            var buffer = new byte[_pageSizeInBytes];
            try
            {
                Reader.Position = PageByteAddress(page);
                int bytesRead = Reader.Read(buffer, 0, _pageSizeInBytes);
                if (bytesRead <= 0)
                {
                    return null;
                }
                if (bytesRead < _pageSizeInBytes)
                {
                    _eof = true;
                    //throw new PageFaultException();
                }
                var tmpPage = PageFromBytes(buffer);
                tmpPage.Address = page;
                if (_count)
                {
                    Program.MainReads++;
                }
                LastPage = tmpPage;
                LastPageNumber = page;
                LastRecordNumber = -1L;
                NextRecordNumber = -1L;
                return tmpPage;
            }
            catch (ArgumentException)
            {
                return null;
            }
            catch (IOException)
            {
                return null;
            }
        }



        public FilePage ReadNextPage()
        {
            var buffer = new byte[_pageSizeInBytes];
            try
            {
                var page = PageNumberFromAddress(Reader.Position);
                int bytesRead = Reader.Read(buffer, 0, _pageSizeInBytes);
                if (bytesRead <= 0)
                {
                    return null;
                }
                if (bytesRead < _pageSizeInBytes)
                {
                    _eof = true;
                    //throw new PageFaultException();
                }
                var tmpPage = PageFromBytes(buffer);
                tmpPage.Address = page;
                if (_count)
                {
                    Program.MainReads++;
                }
                LastPage = tmpPage;
                LastPageNumber = page;
                LastRecordNumber = -1L;
                NextRecordNumber = -1L;
                return tmpPage;
            }
            catch (ArgumentException)
            {
                return null;
            }
            catch (IOException)
            {
                return null;
            }
        }

        public Tuple<Record, long> ReadEntry(long number)
        {
            long page = number / _pageSize;
            int offset = (int)(number % _pageSize);
            if (page == LastPageNumber)
            {
                LastRecordNumber = offset;
                var ret = LastPage.Entries[offset];
                if (ret.Item2 != -1)
                    NextRecordNumber = ret.Item2;
                else
                {
                    NextRecordNumber = -1;
                }
                return ret;
            }
            var newPage = ReadPage(page);
            if (newPage == null)
                return null;

            LastRecordNumber = offset;
            var returning = newPage.Entries[offset];
            if (returning.Item2 != -1)
                NextRecordNumber = returning.Item2;
            else
            {
                NextRecordNumber = -1;
            }
            return returning;
        }
        public Tuple<Record, long> ReadNextEntry()
        {
            long page = _counter / _pageSize;
            int offset = (int)(_counter++ % _pageSize);
            if (page == LastPageNumber)
            {
                LastRecordNumber = offset;
                var ret = LastPage.Entries[offset];
                if (ret.Item2 != -1)
                    NextRecordNumber = ret.Item2;
                else
                {
                    NextRecordNumber = -1;
                }
                return ret;
            }
            var newPage = ReadPage(page);
            if (newPage == null)
                return null;

            LastRecordNumber = offset;
            var returning = newPage.Entries[offset];
            if (returning.Item2 != -1)
                NextRecordNumber = returning.Item2;
            else
            {
                NextRecordNumber = -1;
            }
            return returning;
        }
        public Tuple<Record, long> ReadNextEntryWithChaining()
        {
            if (NextRecordNumber != -1)
            {
                return ReadEntry(NextRecordNumber);
            }
            long page = _counter / _pageSize;
            if (page == PageNumberFromAddress(Index.TempLong))
                return null;
            int offset = (int)(_counter++ % _pageSize);
            if (page == LastPageNumber)
            {
                LastRecordNumber = offset;
                var ret = LastPage.Entries[offset];
                if (ret.Item2 != -1)
                    NextRecordNumber = ret.Item2;
                else
                {
                    NextRecordNumber = -1;
                }
                return ret;
            }
            var newPage = ReadPage(page);
            if (newPage == null)
                return null;

            LastRecordNumber = offset;
            var returning = newPage.Entries[offset];
            if (returning.Item2 != -1)
                NextRecordNumber = returning.Item2;
            else
            {
                NextRecordNumber = -1;
            }
            return returning;
        }
        private FilePage PageFromBytes(byte[] arr)
        {
            var page = new FilePage {Count = BitConverter.ToInt64(arr, 0)};
            var recList = new List<Tuple<Record, long>>();

            for (int i = 8; i < arr.Length; i += 41)
            {
                var del = BitConverter.ToBoolean(arr, i);
                var k = BitConverter.ToInt64(arr, i + 1);
                var a = BitConverter.ToInt64(arr, i + 1 + 8);
                var b = BitConverter.ToInt64(arr, i + 1 + 8*2);
                var c = BitConverter.ToInt64(arr, i + 1 + 8*3);
                var l = BitConverter.ToInt64(arr, i + 1 + 8*4);
                recList.Add(new Tuple<Record, long>(new Record(k,a,b,c,del), l));
            }

            for (int i = 0; i < recList.Count; ++i)
            {
                page.Entries[i] = recList[i];
            }

            return page;
        }

        public void Dispose()
        {
            Reader.Dispose();
            Reader = null;
            LastPage = null;
        }
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ISAM
{
    public class IndexReader : IDisposable
    {
        public BufferedStream Reader;
        public IndexPage LastPage;
        public long LastPageNumber = -1L;
        public long LastRecordNumber = -1L;
        public long NextRecordNumber = -1L;
        private string _path;
        private int _pageSize;
        private bool _eof = false, _count = true;
        private int _pageSizeInBytes { get { return _pageSize * 16 + 8; } }
        private long PageByteAddress(long page)
        {
            return page * (_pageSize * 16 + 8);
        }
        private long PageNumberFromAddress(long address)
        {
            return address / (_pageSize * 16 + 8);
        }
        public IndexReader(string path, int pageSize, Index.Mode m, bool count = true)
        {
            _path = path;
            _pageSize = pageSize;
            var mode = m == Index.Mode.Read ? FileMode.Open : FileMode.Create;
            Reader = new BufferedStream(new FileStream(_path, mode, FileAccess.ReadWrite, FileShare.ReadWrite),
                _pageSizeInBytes);
            _count = count;

        }

        public IndexPage ReadPage(long page)
        {
            if (_eof)
            {
                return LastPage;
            }
            if (page == LastPageNumber)
                return LastPage;

            //Reader.Position = _pageNumberInBytes(page);

            var buffer = new byte[_pageSizeInBytes];
            try
            {
                Reader.Position = PageByteAddress(page);
                int bytesRead = Reader.Read(buffer, 0, _pageSizeInBytes);
                if (bytesRead < _pageSizeInBytes)
                {
                    _eof = true;
                    return null;
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
        public IndexPage ReadNextPage()
        {
            var buffer = new byte[_pageSizeInBytes];
            try
            {
                var page = PageNumberFromAddress(Reader.Position);
                int bytesRead = Reader.Read(buffer, 0, _pageSizeInBytes);
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
        private IndexPage PageFromBytes(byte[] arr)
        {
            var page = new IndexPage();
            page.Count = BitConverter.ToInt64(arr, 0);
            var entries = new List<Tuple<long, long>>();
            for (int i = 8; i < arr.Length; i += 16)
            {
                var k = BitConverter.ToInt64(arr, i);
                var addr = BitConverter.ToInt64(arr, i + 8);
                entries.Add(new Tuple<long, long>(k, addr));
            }
            page.Entries = entries;
            return page;

        }

        public Tuple<long, long> ReadEntry(long number)
        {
            long page = number / _pageSize;
            int offset = (int)(number % _pageSize);
            if (page == LastPageNumber)
                return LastPage.Entries[offset];
            var newPage = ReadPage(page);
            if (newPage == null)
                return null;
            return newPage.Entries[offset];
        }

        public void Dispose()
        {
            Reader.Dispose();
            Reader = null;
            LastPage = null;
        }
    }
}

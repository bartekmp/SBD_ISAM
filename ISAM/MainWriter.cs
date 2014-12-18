using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ISAM
{
    public class MainWriter : IDisposable
    {

        public BufferedStream Writer;
        public FilePage LastPage;
        private long _lastPageNumber = -1L;
        private string _path;
        private int _pageSize;
        private bool _eof = false, _count = true;
        private int _pageSizeInBytes { get { return _pageSize * 41 + 8; } }
        private long PageByteAddress(long page)
        {
            return page * (_pageSize * 41 + 8);
        }

        public void Reset()
        {
            LastPage = null;
            _lastPageNumber = -1;
            _eof = false;

        }

        public MainWriter(string path, int pageSize, Index.Mode m, bool count = true)
        {
            _path = path;
            _pageSize = pageSize;
            var mode = m == Index.Mode.Read ? FileMode.Open : FileMode.Create;
            Writer = new BufferedStream(new FileStream(_path, mode, FileAccess.ReadWrite, FileShare.ReadWrite),
                _pageSizeInBytes);
            _count = count;

        }

        public void WritePage(FilePage filePage)
        {
            _lastPageNumber = filePage.Address;
            LastPage = filePage;
            byte[] buffer = FilePageToBytes(filePage).ToArray();
            Writer.Position = PageByteAddress(_lastPageNumber);
            Writer.Write(buffer, 0, _pageSizeInBytes);
            Writer.Flush();
            if (_count)
            {
                Program.MainWrites++;
            }
        }

        private IEnumerable<byte> FilePageToBytes(FilePage fp)
        {
            var cnt = BitConverter.GetBytes(fp.Count);
            IEnumerable<byte> x = new Byte[0];
            x = x.Concat(cnt);
            for (int i = 0; i < _pageSize; ++i)
            {
                x = x.Concat(fp.Entries[i].Item1.AsBytes());
                x = x.Concat(BitConverter.GetBytes(fp.Entries[i].Item2));
            }
            return x;
        }

        public void Dispose()
        {
            Writer.Dispose();
            Writer = null;
            LastPage = null;
        }
    }
}

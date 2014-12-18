using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ISAM
{
    public class Record
    {
        public const int RecordSize = 4 * sizeof(long);

        private long _a;
        private long _b;
        private long _c;
        private double _sum;
        private long _key;
        private bool _deleted;

        public static Record EmptyRecord()
        {
            var rec = new Record();
            rec._key = long.MaxValue;
            return rec;
        }

        public static IEnumerable<Record> EmptyArray(int count)
        {
            var arr = new Record[count];
            for (int i = 0; i < arr.Length; i++)
                arr[i] = EmptyRecord();
            return arr;
        }


        public Record(long key = 0, long a = 0, long b = 0, long c = 0, bool del = false)
        {
            _a = a;
            _b = b;
            _c = c;
            _key = key;
            _deleted = del;
            try
            {
                _sum = RootsSum();
            }
            catch (NegativeDeltaException)
            {
            }
        }
        public Record(IEnumerable<long> ieLongs)
        {
            var a = ieLongs.ToArray();
            try
            {
                _a = a.ElementAt(1);
                _b = a.ElementAt(2);
                _c = a.ElementAt(3);
                _key = a.ElementAt(0);
                _deleted = false;
                try
                {
                    _sum = RootsSum();
                }
                catch (NegativeDeltaException)
                {
                }
            }
            catch (IndexOutOfRangeException)
            {
            }
        }
        public long A
        {
            get { return _a; }
            set { _a = value; }
        }

        public long B
        {
            get { return _b; }
            set { _b = value; }
        }

        public long C
        {
            get { return _c; }
            set { _c = value; }
        }

        public double RSum
        {
            get { return _sum; }
        }

        public long Key
        {
            get { return _key; }
            set { _key = value; }
        }

        public bool Deleted
        {
            get { return _deleted; }
            set { _deleted = value; }
        }


        public int CompareTo(Record other)
        {
            try
            {
                return _key.CompareTo(other._key);
            }
            catch (NullReferenceException)
            {
                throw;
            }
        }

        protected bool Equals(Record other)
        {
            return _sum.Equals(other._sum);
        }

        public override int GetHashCode()
        {
            return _sum.GetHashCode();
        }

        public static bool operator ==(Record left, Record right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Record left, Record right)
        {
            return !Equals(left, right);
        }

        public static bool operator >(Record lhs, Record rhs)
        {
            return lhs._key > rhs._key;
        }

        public static bool operator >=(Record lhs, Record rhs)
        {
            return lhs._key >= rhs._key;
        }

        public static bool operator <(Record lhs, Record rhs)
        {
            return lhs._key < rhs._key;
        }

        public static bool operator <=(Record lhs, Record rhs)
        {
            return lhs._key <= rhs._key;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Record)obj);
        }

        public override string ToString()
        {
            try
            {
                return string.Format("{0} => {1} {2} {3} {4}", _key, _a, _b, _c, RootsSum());
            }
            catch (NegativeDeltaException)
            {
                return string.Format("{0} => {1} {2} {3} NaN", _key, _a, _b, _c);
            }
            catch (NullReferenceException)
            {
                return string.Format("{0} => {1} {2} {3} NaN", _key, _a, _b, _c);
            }
        }

        /// <summary>
        ///     Method couting roots sum of equation
        /// </summary>
        /// <returns>sum of roots</returns>
        private double RootsSum()
        {
            double Δ = _b * _b - 4 * _a * _c;
            if (Δ < 0)
            {
                throw new NegativeDeltaException("delta is negative");
            }
            if (Math.Abs(_a) < 1e-14)
                return -_c / (double)_b;
            return -_b / (double)_a;
        }

        public IEnumerable<long> AsLongs()
        {
            return new[] { _key, _a, _b, _c };
        }

        public IEnumerable<byte> AsBytes()
        {
            IEnumerable<byte> b = BitConverter.GetBytes(_deleted);
            b = b.Concat(BitConverter.GetBytes(_key));
            b = b.Concat(BitConverter.GetBytes(_a));
            b = b.Concat(BitConverter.GetBytes(_b));
            b = b.Concat(BitConverter.GetBytes(_c));
            return b;
        }
    }
}

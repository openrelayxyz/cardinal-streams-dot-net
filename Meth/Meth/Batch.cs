using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Meth
{
    internal class Batch
    {
        public Dictionary<string, byte[]> SubBatches;  //{"b/s": 0x0000000000000000000000000000000000000000000000000000000000000001}

        public Dictionary<string, byte[]> Updates;

        public List<string> Deletes; //Deletes: ["b/s/4"]

        public byte[] BlockHash;
        public int BatchId;
        public int ItemCount = 0; //defaults to 0; 

        //default constructor sets no values
        public Batch()
        {
            SubBatches = new Dictionary<string, byte[]>(); 
            Updates = new Dictionary<string, byte[]>();
            Deletes = new List<string>();   
        }

        //full constructor requires all values
        public Batch(byte[] blockHash, int batchId, int itemCount, Dictionary<string, byte[]> subbatches, Dictionary<string, byte[]> updates, List<string> deletes)
        {
            SubBatches = subbatches;
            Updates = updates;
            Deletes = deletes;
            BlockHash = blockHash;
            BatchId = batchId;
            ItemCount = itemCount;
        }

    }   //Batch could have methods later if needed, conversion methods from 1 datatype to another 
}

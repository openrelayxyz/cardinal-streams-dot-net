using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Meth
{
    internal class AvroEncoder
    {
        public static byte[] Serialize(object obj)
        {
            using (MemoryStream resultStream = new MemoryStream())
            {
                //do we need a schema?
                var writer = new Avro.IO.BinaryEncoder(resultStream);
                {
                    writer.WriteString(obj.ToString());
                }
                var result = resultStream.ToArray();                
                return result;
            }
        }

    }
}

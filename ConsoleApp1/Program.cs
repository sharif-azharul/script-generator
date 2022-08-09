using System;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            SQLScriptGeneraterColumnSync sQLScriptGenerater = new SQLScriptGeneraterColumnSync();
            sQLScriptGenerater.TableColumnDataMissmatchScripts();
            Console.WriteLine("Done");
        }
    }
}

using System;

namespace KyligenceToolkit
{
    class KyligenceToolkit
    {
        static void Main(string[] args)
        {

            string executionMode = args[0];
            int batch = Convert.ToInt32(args[1]);
            string refreshMode = args[2];
            string projectName = args[3];

            if (executionMode != "auto" && executionMode != "manual")
            {
                Console.WriteLine("ERROR: executionMode must be [auto/manual]");
                return;
            }

            KyligenceProcessor kp = new KyligenceProcessor("kc3", projectName);

            if (refreshMode == "backup")
            {
                kp.ExportModelMetadata();
            }
            else
            {
                kp.ProcessModelBatch(processBatch: batch, refreshMode: refreshMode);
            }

            if (executionMode != "auto")
            {
                Console.WriteLine();
                Console.WriteLine("All tasks compeleted, please press ENTER to exit");
                Console.ReadLine();
            }

        }
        
    }
}

using System.Text;
using Microsoft.Azure.EventHubs;

namespace ProdutorSensor
{
    class Program
    {
        private static EventHubClient? eventHubClient;
        private const string EventHubConnectionString = "Endpoint=sb://xxxxxxxxx.servicebus.windows.net/;SharedAccessKeyName=producer;SharedAccessKey=xxxxxxxxxxxxxx";
        private const string EventHubName = "sonda01";

        static void Main(string[] args)
        {
            //Ajusta para o idioma do SO não interferir na cultura
            System.Globalization.CultureInfo.DefaultThreadCurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
            System.Globalization.CultureInfo.DefaultThreadCurrentUICulture = System.Globalization.CultureInfo.InvariantCulture;
            
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            SendMessagesToEventHub().GetAwaiter().GetResult();

            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();

            eventHubClient.CloseAsync().GetAwaiter().GetResult();
        }

        private static async Task SendMessagesToEventHub()
        {
            string csvFilePath = "C:\\Users\\xxxxxxxxxx\\sonda01.csv";
            string checkpointFilePath = "C:\\Users\\xxxxxxxxxxxx\\checkpoint_sonda01.txt";
            double? timestampCurrentRow = null, timestampPreviousRow = null;

            int lastProcessedRow = 1;

            // Verifica se há um checkpoint salvo
            if (File.Exists(checkpointFilePath))
            {
                lastProcessedRow = int.Parse(File.ReadAllText(checkpointFilePath));
            }

            string[] csvRows = File.ReadAllLines(csvFilePath);

            for (int i = lastProcessedRow; i < csvRows.Length; i++)
            {
                try
                {
                    var message = Sonda01.Message(csvRows[i]);
                    timestampCurrentRow = Sonda01.Timestamp;
                    eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                    //armazena a última linha processada no arquivo de checkpoint
                    File.WriteAllText(checkpointFilePath, i.ToString());
                    if(timestampPreviousRow.HasValue && timestampCurrentRow != timestampPreviousRow) {
                        int delayMs = (int)(timestampCurrentRow - timestampPreviousRow);
                        await Task.Delay(delayMs);
                    }
                    timestampPreviousRow = timestampCurrentRow;
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"Exceção: {exception.Message}");
                }
            }

        }
    }

    public class Sonda01
    {
        public static double? Timestamp { get; set; }

        public static string Message(string csvRow)
        {
            //csvRow = csvRow.Replace("\\\"", null);
            string?[] values = csvRow.Split(',');
            Timestamp = Convert.ToDouble(values[5]);
            for(int i = 0; i<values.Length; i++) {
                values[i] = string.IsNullOrEmpty(values[i]) ? "null" : values[i];
            }
            string message = $"{{\"__type\":{values[0]},\"__src\":{values[1]},\"mnemonic\":{values[2]},\"value\":{values[3]},\"Uom\":{values[4]},";
            message += $"\"index_timestamp\":{values[5]},\"adjusted_index_timestamp\":{values[6]},\"index_type\":{values[7]},\"well_name\":{values[8]},";
            message += $"\"extra\":{values[9]},\"config\":{values[10]},\"timestamp\":{values[11]},\"live__parquet__others\":{values[12]},\"errors\":{values[13]},\"_rescued_data\":{values[14]}}}";

            return message;
        }
        
    }
}
using System.ComponentModel.DataAnnotations;
using System.Text;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;

namespace ProdutorSensor
{
    class Program
    {
        private static EventHubClient? eventHubClient;
        private const string EventHubConnectionString = "Endpoint=sb://xxxxxxxxx.servicebus.windows.net/;SharedAccessKeyName=producer;SharedAccessKey=xxxxxxxxxxxxxx";
        private const string EventHubName = "sonda02";

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
            string csvFilePath = "C:\\Users\\xxxxxxxxxxxx\\sonda02.csv";
            string checkpointFilePath = "C:\\Users\\xxxxxxxxxx\\checkpoint_sonda02.txt";
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
                    var message = Sonda02.Message(csvRows[i]);
                    timestampCurrentRow = Sonda02.Timestamp;
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
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

    public class Sonda02
    {
        public static double? Timestamp { get; set; }

        public static string Message(string csvRow)
        {
            string?[] values = csvRow.Split(',');
            List<string?> valuesList = values.ToList();
            if (valuesList.Count > 21)
            {
                int startIndex = 13;
                int endIndex = valuesList.FindIndex(startIndex, item => item.Contains("]}"));

                if (endIndex != -1)
                {
                    valuesList[startIndex] = string.Join(",", valuesList.GetRange(startIndex, endIndex - startIndex + 1));
                    valuesList.RemoveRange(startIndex + 1, endIndex - startIndex);
                }
            }

            Timestamp = Convert.ToDouble(valuesList[0]);
            for(int i = 0; i< valuesList.Count; i++) {
                valuesList[i] = string.IsNullOrEmpty(valuesList[i]) ? "null" : valuesList[i];
            }


            string message = $"{{\"index_timestamp\":{valuesList[0]},\"adjusted_index_timestamp\":{valuesList[1]}," +
                $"\"index_type\":{valuesList[2]},\"uom\":{valuesList[3]},\"extra\":{valuesList[4]}," +
                $"\"mnemonic\":{valuesList[5]},\"well_name\":{valuesList[6]},\"value\":{valuesList[7]}," +
                $"\"config\":{valuesList[8]},\"errors\":{valuesList[9]},\"__type\":{valuesList[10]}," +
                $"\"__src\":{valuesList[11]},\"timestamp\":{valuesList[12]},\"live__parquet__others\":{valuesList[13]}," +
                $"\"index_mnemonic\":{valuesList[14]},\"index_value\":{valuesList[15]},\"index_uom\":{valuesList[16]}," +
                $"\"depth_mnemonic\":{valuesList[17]},\"depth_value\":{valuesList[18]},\"depth_uom\":{valuesList[19]}," +
                $"\"_rescued_data\":{valuesList[20]}}}";

            return message;
        }
        
    }
}
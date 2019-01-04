using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace PerformanceLogSplitter
{
    class Program
    {
        /// <summary>
        /// IP正则
        /// </summary>
        static readonly Regex IPRegex = new Regex(
            @"^[\d-\s:\.]{23}\s(?<IPAddress>\d{1,3}(\.\d{1,3}){3})\s.*$",
            RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("请传入Performance日志文件存放路径...");
                Environment.Exit(-1);
            }

            string logDir = args[0];
            if (!Directory.Exists(logDir))
            {
                Console.WriteLine($"未找到日志文件存放目录：{logDir}");
                Environment.Exit(-2);
            }

            Console.WriteLine($"开始拆分，工作目录：{logDir}");
            // 并行拆分所有文件
            Directory.GetFiles(logDir, "PerformanceLog????????.txt", SearchOption.AllDirectories).AsParallel().ForAll(path => SplitPerformanceLogFile(path));

            Console.ReadLine();
        }

        /// <summary>
        /// 拆分Performance日志文件
        /// </summary>
        /// <param name="path"></param>
        private static void SplitPerformanceLogFile(string path)
        {
            if (!File.Exists(path))
            {
                Console.WriteLine($"不存在的文件：{path}");
                return;
            }

            Console.WriteLine($"开始拆分文件：{path}");
            ConcurrentDictionary<string, ConcurrentBag<string>> IPLogs = new ConcurrentDictionary<string, ConcurrentBag<string>>();

            using (FileStream logStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                using (StreamReader logReader = new StreamReader(logStream, Encoding.Default))
                {
                    string logContent = string.Empty,
                        IPaddress = string.Empty;
                    ConcurrentBag<string> currentBag = null;

                    while (!logReader.EndOfStream)
                    {
                        logContent = logReader.ReadLine();

                        IPaddress = IPRegex.Match(logContent).Groups["IPAddress"].Value;

                        if (!IPLogs.ContainsKey(IPaddress))
                        {
                            currentBag = new ConcurrentBag<string>();
                            IPLogs[IPaddress] = currentBag;
                        }
                        else
                        {
                            currentBag = IPLogs[IPaddress];
                        }

                        currentBag.Add(logContent);
                    }
                }
            }
            Console.WriteLine($"读取完成：{path}, 共 {IPLogs.Count} 个 IP 的 {IPLogs.Sum(bag => bag.Value.Count)} 条日志。");

            IPLogs.AsParallel().ForAll(logsPair =>
            {
                string ip = logsPair.Key;
                ConcurrentBag<string> logs = logsPair.Value;
                string targetPath = $"{path}-{ip}.txt";
                Console.WriteLine($"导出=> {targetPath}");

                using (FileStream splitStream = new FileStream(targetPath, FileMode.Create))
                {
                    using (StreamWriter splitWriter = new StreamWriter(splitStream, Encoding.Default))
                    {
                        foreach (string log in logs)
                        {
                            splitWriter.WriteLine(log);
                        }
                    }
                }
            });

            Console.WriteLine($"拆分完毕：{path}");
        }
    }
}

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

        /// <summary>
        /// 日志池
        /// </summary>
        static ConcurrentDictionary<string, ConcurrentBag<string>> IPLogs = new ConcurrentDictionary<string, ConcurrentBag<string>>();

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
            Directory.GetFiles(logDir, "PerformanceLog*.txt", SearchOption.AllDirectories).AsParallel().ForAll(path => SplitPerformanceLogFile(path));

            ShowPoolState();

            ExportPoolToFile(logDir);

            Console.WriteLine("任务完成！");
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
            try
            {
                using (FileStream logStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    using (StreamReader logReader = new StreamReader(logStream, Encoding.Default))
                    {
                        int count = 0;
                        string logContent = string.Empty,
                            IPaddress = string.Empty;
                        ConcurrentBag<string> currentBag = null;

                        while (!logReader.EndOfStream)
                        {
                            logContent = logReader.ReadLine();

                            IPaddress = IPRegex.Match(logContent).Groups["IPAddress"].Value;
                            count++;

                            // 获取或新建IP对应的日志池
                            currentBag = IPLogs.GetOrAdd(IPaddress, new ConcurrentBag<string>());
                            currentBag.Add(logContent);
                        }

                        Console.WriteLine($"读取完成：{path}，共 {count} 行日志");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"读取文件遇到异常：{path}\n{ex.Message}");
            }
        }

        /// <summary>
        /// 输出日志池状态
        /// </summary>
        private static void ShowPoolState()
        {
            Console.WriteLine($"日志文件解析完成，共 {IPLogs.Count} 个IP的 {IPLogs.Sum(bag => bag.Value.Count)} 条日志。");
        }

        /// <summary>
        /// 导出日志池
        /// </summary>
        /// <param name="exportDir"></param>
        private static void ExportPoolToFile(string exportDir)
        {
            IPLogs.AsParallel().ForAll(logsPair =>
            {
                string ip = logsPair.Key;
                ConcurrentBag<string> logs = logsPair.Value;
                string targetPath = Path.Combine(exportDir, $"PerformanceLog.{ip}.txt");
                Console.WriteLine($"导出=> {targetPath}");

                try
                {
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
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"导出文件遇到异常：{targetPath}\n{ex.Message}");
                }
            });
        }
    }
}

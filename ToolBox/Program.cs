/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Lean.DataSource.Polygon;
using QuantConnect.Logging;
using QuantConnect.ToolBox.AlgoSeekFuturesConverter;
using QuantConnect.ToolBox.CoarseUniverseGenerator;
using QuantConnect.ToolBox.KaikoDataConverter;
using QuantConnect.ToolBox.RandomDataGenerator;
using QuantConnect.Util;
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using static QuantConnect.Configuration.ApplicationParser;

namespace QuantConnect.ToolBox
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var optionsObject = ToolboxArgumentParser.ParseArguments(args);

            if (optionsObject.TryGetValue("config", out var configValue))
            {
                var configPath = configValue?.ToString();
                if (!string.IsNullOrWhiteSpace(configPath))
                {
                    Config.SetConfigurationFile(configPath);
                    Config.Reset();
                }
                optionsObject.Remove("config");
            }

            Log.DebuggingEnabled = Config.GetBool("debug-mode");
            var destinationDir = Config.Get("results-destination-folder");
            if (!string.IsNullOrEmpty(destinationDir))
            {
                Directory.CreateDirectory(destinationDir);
                Log.FilePath = Path.Combine(destinationDir, "log.txt");
            }
            Log.LogHandler = Composer.Instance.GetExportedValueByTypeName<ILogHandler>(Config.Get("log-handler", "CompositeLogHandler"));

            if (optionsObject.Count == 0)
            {
                PrintMessageAndExit();
            }

            var dataProvider
                = Composer.Instance.GetExportedValueByTypeName<IDataProvider>(Config.Get("data-provider", "DefaultDataProvider"));
            var mapFileProvider
                = Composer.Instance.GetExportedValueByTypeName<IMapFileProvider>(Config.Get("map-file-provider", "LocalDiskMapFileProvider"));
            var factorFileProvider
                = Composer.Instance.GetExportedValueByTypeName<IFactorFileProvider>(Config.Get("factor-file-provider", "LocalDiskFactorFileProvider"));

            mapFileProvider.Initialize(dataProvider);
            factorFileProvider.Initialize(mapFileProvider, dataProvider);

            var targetApp = GetParameterOrExit(optionsObject, "app").ToLowerInvariant();
            if (targetApp.Contains("download") || targetApp.EndsWith("dl"))
            {
                var fromDate = Parse.DateTimeExact(GetParameterOrExit(optionsObject, "from-date"), "yyyyMMdd-HH:mm:ss");
                var resolution = optionsObject.ContainsKey("resolution") ? optionsObject["resolution"].ToString() : "";
                var market = optionsObject.ContainsKey("market") ? optionsObject["market"].ToString() : "";
                var securityType = optionsObject.ContainsKey("security-type") ? optionsObject["security-type"].ToString() : "";
                var tickers = ToolboxArgumentParser.GetTickers(optionsObject);
                var toDate = optionsObject.ContainsKey("to-date")
                    ? Parse.DateTimeExact(optionsObject["to-date"].ToString(), "yyyyMMdd-HH:mm:ss")
                    : DateTime.UtcNow;
                switch (targetApp)
                {
                    case "polygondatadownloader":
                    case "polygondl":
                    case "polygon":
                        RunPolygonDataDownloader(tickers, market, securityType, resolution, fromDate, toDate);
                        break;

                    default:
                        PrintMessageAndExit(1, "ERROR: Unrecognized --app value");
                        break;
                }
            }
            else
            {
                switch (targetApp)
                {
                    case "asfc":
                    case "algoseekfuturesconverter":
                        AlgoSeekFuturesProgram.AlgoSeekFuturesConverter(GetParameterOrExit(optionsObject, "date"));
                        break;
                    case "kdc":
                    case "kaikodataconverter":
                        KaikoDataConverterProgram.KaikoDataConverter(GetParameterOrExit(optionsObject, "source-dir"),
                                                                     GetParameterOrExit(optionsObject, "date"),
                                                                     GetParameterOrDefault(optionsObject, "exchange", string.Empty));
                        break;
                    case "cug":
                    case "coarseuniversegenerator":
                        CoarseUniverseGeneratorProgram.CoarseUniverseGenerator();
                        break;
                    case "rdg":
                    case "randomdatagenerator":
                        var tickers = ToolboxArgumentParser.GetTickers(optionsObject);
                        RandomDataGeneratorProgram.RandomDataGenerator(
                            GetParameterOrExit(optionsObject, "start"),
                            GetParameterOrExit(optionsObject, "end"),
                            GetParameterOrDefault(optionsObject, "symbol-count", null),
                            GetParameterOrDefault(optionsObject, "market", null),
                            GetParameterOrDefault(optionsObject, "security-type", "Equity"),
                            GetParameterOrDefault(optionsObject, "resolution", "Minute"),
                            GetParameterOrDefault(optionsObject, "data-density", "Dense"),
                            GetParameterOrDefault(optionsObject, "include-coarse", "true"),
                            GetParameterOrDefault(optionsObject, "quote-trade-ratio", "1"),
                            GetParameterOrDefault(optionsObject, "random-seed", null),
                            GetParameterOrDefault(optionsObject, "ipo-percentage", "5.0"),
                            GetParameterOrDefault(optionsObject, "rename-percentage", "30.0"),
                            GetParameterOrDefault(optionsObject, "splits-percentage", "15.0"),
                            GetParameterOrDefault(optionsObject, "dividends-percentage", "60.0"),
                            GetParameterOrDefault(optionsObject, "dividend-every-quarter-percentage", "30.0"),
                            GetParameterOrDefault(optionsObject, "option-price-engine", "BaroneAdesiWhaleyApproximationEngine"),
                            GetParameterOrDefault(optionsObject, "volatility-model-resolution", "Daily"),
                            GetParameterOrDefault(optionsObject, "chain-symbol-count", "1"),
                            tickers
                        );
                        break;

                    default:
                        PrintMessageAndExit(1, "ERROR: Unrecognized --app value");
                        break;
                }
            }
        }

        private static void RunPolygonDataDownloader(List<string> tickers, string market, string securityType, string resolution, DateTime fromDate, DateTime toDate)
        {
            if (tickers == null || tickers.Count == 0)
            {
                PrintMessageAndExit(1, "ERROR: --tickers is required for PolygonDataDownloader");
            }

            if (string.IsNullOrWhiteSpace(securityType))
            {
                securityType = SecurityType.Equity.ToString();
            }

            if (string.IsNullOrWhiteSpace(resolution))
            {
                resolution = Resolution.Minute.ToString();
            }

            if (!Enum.TryParse(securityType, true, out SecurityType securityTypeEnum))
            {
                PrintMessageAndExit(1, $"ERROR: Unsupported security-type '{securityType}'.");
            }

            if (!Enum.TryParse(resolution, true, out Resolution resolutionEnum))
            {
                PrintMessageAndExit(1, $"ERROR: Unsupported resolution '{resolution}'.");
            }

            if (string.IsNullOrWhiteSpace(market))
            {
                market = Market.USA;
            }
            else
            {
                market = market.ToLowerInvariant();
            }

            var dataFolder = Config.Get("data-folder", Globals.DataFolder);
            var tickType = securityTypeEnum switch
            {
                SecurityType.Forex => TickType.Quote,
                SecurityType.Cfd => TickType.Quote,
                SecurityType.Crypto => TickType.Quote,
                SecurityType.Option when resolutionEnum == Resolution.Tick => TickType.Trade,
                _ => TickType.Trade
            };

            using var downloader = new PolygonDataDownloader();

            foreach (var rawTicker in tickers)
            {
                var ticker = rawTicker.Trim();
                if (string.IsNullOrEmpty(ticker))
                {
                    continue;
                }

                var symbol = Symbol.Create(ticker, securityTypeEnum, market);
                var start = DateTime.SpecifyKind(fromDate, DateTimeKind.Utc);
                var end = DateTime.SpecifyKind(toDate, DateTimeKind.Utc);

                var request = new DataDownloaderGetParameters(symbol, resolutionEnum, start, end, tickType);
                var data = downloader.Get(request);

                if (data == null)
                {
                    Log.Error($"PolygonDataDownloader: no data returned for {symbol}");
                    continue;
                }

                var orderedData = data.OrderBy(point => point.EndTime).ToList();
                if (orderedData.Count == 0)
                {
                    Log.Error($"PolygonDataDownloader: empty data set for {symbol}");
                    continue;
                }

                var writer = new LeanDataWriter(resolutionEnum, symbol, dataFolder, tickType);
                writer.Write(orderedData);
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using NewLife;
using NewLife.Caching;
using NewLife.Core.Collections;
using NewLife.Http;
using NewLife.Log;
using NewLife.Net;
using NewLife.Reflection;
using NewLife.Remoting;
using NewLife.Security;
using NewLife.Serialization;
using XCode.Code;
using XCode.DataAccessLayer;
using XCode.Extension;
using XCode.Membership;
using XCode.Service;
#if !NET4
using TaskEx = System.Threading.Tasks.Task;
#endif

namespace Test
{
    public class Program
    {
        private static void Main(String[] args)
        {
            //XTrace.Log = new NetworkLog();
            XTrace.UseConsole();
#if DEBUG
            XTrace.Debug = true;
#endif
            while (true)
            {
                var sw = Stopwatch.StartNew();
#if !DEBUG
                try
                {
#endif
                Test21();
#if !DEBUG
                }
                catch (Exception ex)
                {
                    XTrace.WriteException(ex?.GetTrue());
                }
#endif

                sw.Stop();
                Console.WriteLine("OK! 耗时 {0}", sw.Elapsed);
                //Thread.Sleep(5000);
                GC.Collect();
                GC.WaitForPendingFinalizers();
                var key = Console.ReadKey(true);
                if (key.Key != ConsoleKey.C) break;
            }
        }

        static async void Test1()
        {
            DbCache dbCache = new DbCache(MyDbCache.Meta.Factory, MyDbCache.__.Group, MyDbCache.__.Name, MyDbCache.__.ExpiredTime);
            MyDbCache.Meta.Factory.Session.Dal.Db.ShowSQL = true;
            IList<UserX> list = dbCache.GetGroup<UserX>("UserX");
            IList<UserX> list1 = dbCache.GetGroup<UserX>("UserX1");
            IList<String> @group = dbCache.GetGroup<string>("UserX");
            //            dbCache.RemoveGroup("UserX");
            //            dbCache.RemoveGroup("UserX1");
            list = new List<UserX>()
            {
                new UserX(){
                    ID     = 1,
                    Name   = "aaa",
                    Avatar = "aaa1"
                },new UserX(){
                    ID     = 1,
                    Name   = "aaa",
                    Avatar = "aaa2"
                },new UserX(){
                    ID     = 1,
                    Name   = "aaa",
                    Avatar = "aaa3"
                },
                new UserX(){
                    ID     = 2,
                    Name   = "bbb",
                    Avatar = "bbb1"
                },
            };
            dbCache.AddGroup("UserX", list, x => x.ID, 100);

            dbCache.AddGroup("UserX1", list, x => x.ID, 100);
            dbCache.Remove("UserX", new[] { "1" });
            dbCache.SetGroup("UserX", list, x => x.ID, 100);
            dbCache.SetGroup("UserX1", list, x => x.ID, 100);
            IList<Int32> ints = dbCache.GetGroup<int>("UserX");
            dbCache["ttt##1"] = new MyDbCache() { Group = "ttt", Name = "1", Value = "aaa", CreateTime = DateTime.Now, ExpiredTime = DateTime.Now.AddMinutes(5) };
            dbCache.RemoveGroup("UserX1");
            dbCache.Add("bbb", "2", new UserX()
            {
                ID = 2,
                Name = "bbb",
                Avatar = "bbb1"
            }, TimeSpan.FromMinutes(5));
            Console.Read();
            dbCache.GetGroup<UserX>("bbb");
            dbCache.AddGroup("ttt", new[] { "a_1", "b_2", "c_3" }, x => x.Split("_")[0], 100);
            IList<String> group1 = dbCache.GetGroup<string>("ttt");

            dbCache.AddGroup("ddd", new[] { 1001, 2002, 3003 }, x => x.ToString()[0], 1000);
            IList<int> group2 = dbCache.GetGroup<int>("ddd");

            List<Int32> limit = Enumerable.Range(1, 1000).ToList();
            for (int i = 1; i <= 10; i++)
            {
                string gpk = $"Group_{i}";
                List<UserX> teamUsers = limit.AsParallel().Select(r => new UserX()
                {
                    ID = r,
                    Name = $"User_{r}",
                }).ToList();
                dbCache.SetGroup(gpk, teamUsers, x => x.ID, 10);
            }
            Task.Run(() =>
            {
                int i = 0;
                while (true)
                {
                    Console.WriteLine($"{i}:{dbCache.Count}");
                    Thread.Sleep(1000);
                    i++;
                }
            });
            Console.WriteLine("1111:"+Console.ReadLine());
            Console.WriteLine("2222:"+Console.ReadLine());
            Console.WriteLine("3333:"+Console.ReadLine());
        }

        private static void Test20()
        {
            DAL.AddConnStr("MuchNewDb", "Data Source=DBStage1.tutorabc.com;Initial Catalog=muchnewdb;User ID=VipjrMemberApiAcct;Password=Vede2EchUB5#;", null, "sqlserver");
            DAL dal = DAL.Create("MuchNewDb");
            List<IDataTable> dataTables = dal.Tables;
        }

        static void Test1()
        {
            //new AgentService().Main();

            //var wc = new WebClientX
            //{
            //    Log = XTrace.Log
            //};
            //var url = wc.DownloadLink("http://x.newlifex.com/", "Oracle.ManagedDataAccess.st", ".");
            //XTrace.WriteLine(url);

            //url = wc.DownloadLink("http://x.newlifex.com/", "MySql.Data.st", ".");
            //XTrace.WriteLine(url);

            //url = wc.DownloadLink("http://x.newlifex.com/", "MySql.Data64Fx40,MySql.Data", ".");
            //XTrace.WriteLine(url);

            //url = wc.DownloadLink("http://x.newlifex.com/", "System.Data.SqlClient.st", ".");
            //XTrace.WriteLine(url);

            //VisitStat.Meta.Session.Dal.Db.ShowSQL = true;

            //var vs = VisitStat.FindByID(1) ?? new VisitStat();
            //vs.Times += 123;
            //vs.Users++;
            //vs.IPs++;

            //vs.Save();

            XTrace.Log.Level = LogLevel.All;

            using (var tran = UserX.Meta.CreateTrans())
            {
                var user = UserX.FindByKey(1);
                XTrace.WriteLine(user.Logins + "");

                user.Logins++;

                user.Save();

                //tran.Commit();
            }

            {
                var user = UserX.FindByKey(1);
                XTrace.WriteLine(user.Logins + "");
            }
        }

        static void Test2()
        {
            //DAL.AddConnStr("Log", "Data Source=tcp://127.0.0.1/ORCL;User Id=scott;Password=tiger;UseParameter=true", null, "Oracle");
            //DAL.AddConnStr("Log", "Server=.;Port=3306;Database=Log;Uid=root;Pwd=root;", null, "MySql");
            //DAL.AddConnStr("Membership", "Server=.;Port=3306;Database=times;Uid=root;Pwd=Pass@word;TablePrefix=xx_", null, "MySql");
            //DAL.AddConnStr("Membership", @"Server=.\JSQL2008;User ID=sa;Password=sa;Database=Membership;", null, "sqlserver");
            //DAL.AddConnStr("Log", @"Server=.\JSQL2008;User ID=sa;Password=sa;Database=Log;", null, "sqlserver");

            UserX.Meta.Session.Dal.Db.ShowSQL = true;
            Log.Meta.Session.Dal.Db.ShowSQL = true;

            var gs = UserX.FindAll(null, null, null, 0, 10);
            Console.WriteLine(gs.First().Logins);
            var count = UserX.FindCount();
            Console.WriteLine("Count={0}", count);

            LogProvider.Provider.WriteLog("test", "新增", "学无先后达者为师");
            LogProvider.Provider.WriteLog("test", "新增", "学无先后达者为师");
            LogProvider.Provider.WriteLog("test", "新增", "学无先后达者为师");

            var list = new List<UserX>();
            for (var i = 0; i < 4; i++)
            {
                var entity = new UserX
                {
                    Name = "Stone" + i,
                    DisplayName = "大石头" + i,
                    Logins = 1,
                    LastLogin = DateTime.Now,
                    RegisterTime = DateTime.Now
                };
                list.Add(entity);
                entity.SaveAsync();
                //entity.InsertOrUpdate();
            }
            //list.Save();


            //Console.WriteLine(client.BaseAddress);

            //Console.WriteLine(uri);
            //Console.WriteLine(client.BaseAddress == uri);

            var client = new HttpClient();
            client.BaseAddress = new Uri("http://feifan.link:2233");

            var rs = client.Invoke<Object>("api/info");
            Console.WriteLine(rs.ToJson(true));

            rs = client.Invoke<Object>("api/info3", rs);
            Console.WriteLine(rs.ToJson(true));
        }

        static void Test3()
        {
            //XTrace.WriteLine("IsConsole={0}", Runtime.IsConsole);
            //Console.WriteLine("IsConsole={0}", Runtime.IsConsole);
            //XTrace.WriteLine("MainWindowHandle={0}", Process.GetCurrentProcess().MainWindowHandle);

            if (Console.ReadLine() == "1")
            {
                var svr = new ApiServer(1234)
                //var svr = new ApiServer("http://*:1234")
                {
                    Log = XTrace.Log,
                    //EncoderLog = XTrace.Log,
                    StatPeriod = 10,
                };

                var ns = svr.EnsureCreate() as NetServer;
                ns.EnsureCreateServer();
                var ts = ns.Servers.FirstOrDefault(e => e is TcpServer);
                //ts.ProcessAsync = true;

                svr.Start();

                Console.ReadKey();
            }
            else
            {
                var client = new ApiClient("tcp://127.0.0.1:335,tcp://127.0.0.1:1234")
                {
                    Log = XTrace.Log,
                    //EncoderLog = XTrace.Log,
                    StatPeriod = 10,

                    UsePool = true,
                };
                client.Open();

                TaskEx.Run(() =>
                {
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        for (var i = 0; i < 10; i++)
                        {
                            client.InvokeAsync<Object>("Api/All", new { state = 111 }).Wait();
                        }
                    }
                    catch (Exception ex)
                    {
                        XTrace.WriteException(ex.GetTrue());
                    }
                    sw.Stop();
                    XTrace.WriteLine("总耗时 {0:n0}ms", sw.ElapsedMilliseconds);
                });

                TaskEx.Run(() =>
                {
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        for (var i = 0; i < 10; i++)
                        {
                            client.InvokeAsync<Object>("Api/All", new { state = 222 }).Wait();
                        }
                    }
                    catch (Exception ex)
                    {
                        XTrace.WriteException(ex.GetTrue());
                    }
                    sw.Stop();
                    XTrace.WriteLine("总耗时 {0:n0}ms", sw.ElapsedMilliseconds);
                });

                TaskEx.Run(() =>
                {
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        for (var i = 0; i < 10; i++)
                        {
                            client.InvokeAsync<Object>("Api/Info", new { state = 333 }).Wait();
                        }
                    }
                    catch (Exception ex)
                    {
                        XTrace.WriteException(ex.GetTrue());
                    }
                    sw.Stop();
                    XTrace.WriteLine("总耗时 {0:n0}ms", sw.ElapsedMilliseconds);
                });

                TaskEx.Run(() =>
                {
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        for (var i = 0; i < 10; i++)
                        {
                            client.InvokeAsync<Object>("Api/Info", new { state = 444 }).Wait();
                        }
                    }
                    catch (Exception ex)
                    {
                        XTrace.WriteException(ex.GetTrue());
                    }
                    sw.Stop();
                    XTrace.WriteLine("总耗时 {0:n0}ms", sw.ElapsedMilliseconds);
                });

                Console.ReadKey();
            }
        }

        static void Test4()
        {
            var v = Rand.NextBytes(32);
            Console.WriteLine(v.ToBase64());

            ICache ch = null;
            //ICache ch = new DbCache();
            //ch.Set(key, v);
            //v = ch.Get<Byte[]>(key);
            //Console.WriteLine(v.ToBase64());
            //ch.Remove(key);

            Console.Clear();

            Console.Write("选择要测试的缓存：1，MemoryCache；2，DbCache；3，Redis ");
            var select = Console.ReadKey().KeyChar;
            switch (select)
            {
                case '1':
                    ch = new MemoryCache();
                    break;
                case '2':
                    ch = new DbCache();
                    break;
                case '3':
                    ch = Redis.Create("127.0.0.1", 9);
                    break;
            }

            var mode = false;
            Console.WriteLine();
            Console.Write("选择测试模式：1，顺序；2，随机 ");
            if (Console.ReadKey().KeyChar != '1') mode = true;

            Console.Clear();

            ch.Bench(mode);
        }

        static void Test5()
        {
            var set = XCode.Setting.Current;
            set.Debug = true;
            set.ShowSQL = true;

            Console.WriteLine("1，服务端；2，客户端");
            if (Console.ReadKey().KeyChar == '1')
            {
                var n = UserOnline.Meta.Count;

                var svr = new DbServer
                {
                    Log = XTrace.Log,
                    StatPeriod = 5
                };
                svr.Start();
            }
            else
            {
                DAL.AddConnStr("net", "Server=tcp://admin:newlife@127.0.0.1:3305/Log", null, "network");
                var dal = DAL.Create("net");

                UserOnline.Meta.ConnName = "net";

                var count = UserOnline.Meta.Count;
                Console.WriteLine("count={0}", count);

                var entity = new UserOnline
                {
                    Name = "新生命",
                    OnlineTime = 12345
                };
                entity.Insert();

                Console.WriteLine("id={0}", entity.ID);

                var entity2 = UserOnline.FindByKey(entity.ID);
                Console.WriteLine("user={0}", entity2);

                entity2.Page = Rand.NextString(8);
                entity2.Update();

                entity2.Delete();

                for (var i = 0; i < 100; i++)
                {
                    entity2 = new UserOnline
                    {
                        Name = Rand.NextString(8),
                        Page = Rand.NextString(8)
                    };
                    entity2.Insert();

                    Thread.Sleep(5000);
                }
            }

            //var client = new DbClient();
            //client.Log = XTrace.Log;
            //client.EncoderLog = client.Log;
            //client.StatPeriod = 5;

            //client.Servers.Add("tcp://127.0.0.1:3305");
            //client.Open();

            //var db = "Membership";
            //var rs = client.LoginAsync(db, "admin", "newlife").Result;
            //Console.WriteLine((DatabaseType)rs["DbType"].ToInt());

            //var ds = client.QueryAsync("Select * from User").Result;
            //Console.WriteLine(ds);

            //var count = client.QueryCountAsync("User").Result;
            //Console.WriteLine("count={0}", count);

            //var ps = new Dictionary<String, Object>
            //{
            //    { "Logins", 3 },
            //    { "id", 1 }
            //};
            //var es = client.ExecuteAsync("update user set Logins=Logins+@Logins where id=@id", ps).Result;
            //Console.WriteLine("Execute={0}", es);
        }

        private static NetServer _netServer;
        static void Test6()
        {
            var pfx = new X509Certificate2("../newlife.pfx", "newlife");
            //Console.WriteLine(pfx);

            //using var svr = new ApiServer(1234);
            //svr.Log = XTrace.Log;
            //svr.EncoderLog = XTrace.Log;

            //var ns = svr.EnsureCreate() as NetServer;

            using var ns = new NetServer(1234)
            {
                Name = "Server",
                ProtocolType = NetType.Tcp,
                Log = XTrace.Log,
                SessionLog = XTrace.Log,
                SocketLog = XTrace.Log,
                LogReceive = true
            };

            ns.EnsureCreateServer();
            foreach (var item in ns.Servers)
            {
                if (item is TcpServer ts) ts.Certificate = pfx;
            }

            ns.Received += (s, e) =>
            {
                XTrace.WriteLine("收到：{0}", e.Packet.ToStr());
            };
            ns.Start();

            using var client = new TcpSession
            {
                Name = "Client",
                Remote = new NetUri("tcp://127.0.0.1:1234"),
                SslProtocol = SslProtocols.Tls,
                Log = XTrace.Log,
                LogSend = true
            };
            client.Open();

            client.Send("Stone");

            Console.ReadLine();
        }
        static void Test7()
        {
            Role.Meta.Session.Dal.Db.ShowSQL = true;
            Role.Meta.Session.Dal.Expire = 10;
            //Role.Meta.Session.Dal.Db.Readonly = true;

            var list = Role.FindAll();
            Console.WriteLine(list.Count);

            list = Role.FindAll(Role._.Name.NotContains("abc"));
            Console.WriteLine(list.Count);

            Thread.Sleep(1000);

            list = Role.FindAll();
            Console.WriteLine(list.Count);

            Thread.Sleep(1000);

            var r = list.Last();
            r.IsSystem = !r.IsSystem;
            r.Update();

            Thread.Sleep(5000);

            list = Role.FindAll();
            Console.WriteLine(list.Count);
        }

        static void Test8()
        {
            var ss = new String[8];
            ss[1] = "Stone";
            ss[3] = "NewLife";
            var str = ss.Join();
            Console.WriteLine(str);
        }

        static async void Test9()
        {
            //var rds = new Redis();
            //rds.Server = "127.0.0.1";
            //if (rds.Pool is ObjectPool<RedisClient> pp) pp.Log = XTrace.Log;
            //rds.Bench();

            //Console.ReadKey();

            var svr = new ApiServer(3379)
            {
                Log = XTrace.Log
            };
            svr.Start();

            var client = new ApiClient("tcp://127.0.0.1:3379")
            {
                Log = XTrace.Log
            };
            client.Open();

            for (var i = 0; i < 10; i++)
            {
                XTrace.WriteLine("Invoke {0}", i);
                var sw = Stopwatch.StartNew();
                var rs = await client.InvokeAsync<String[]>("Api/All");
                sw.Stop();
                XTrace.WriteLine("{0}=> {1:n0}us", i, sw.Elapsed.TotalMilliseconds * 1000);
                //XTrace.WriteLine(rs.Join(","));
            }

            Console.WriteLine();
            Parallel.For(0, 10, async i =>
            {
                XTrace.WriteLine("Invoke {0}", i);
                var sw = Stopwatch.StartNew();
                var rs = await client.InvokeAsync<String[]>("Api/All");
                sw.Stop();
                XTrace.WriteLine("{0}=> {1:n0}us", i, sw.Elapsed.TotalMilliseconds * 1000);
                //XTrace.WriteLine(rs.Join(","));
            });
        }

        static void Test10()
        {
            var dt1 = new DateTime(1970, 1, 1);
            //var x = dt1.ToFileTimeUtc();

            var yy = Int64.Parse("-1540795502468");

            //var yy = "1540795502468".ToInt();
            Console.WriteLine(yy);

            var dt = 1540795502468.ToDateTime();
            var y = dt.ToUniversalTime();
            Console.WriteLine(dt1.ToLong());
        }

        static void Test11()
        {
            var xmlFile = Path.Combine(Directory.GetCurrentDirectory(), "../X/XCode/Model.xml");
            var output = Path.Combine(Directory.GetCurrentDirectory(), "../");
            EntityBuilder.Build(xmlFile, output);
        }

        /// <summary>测试序列化</summary>
        static void Test12()
        {
            var bdic = new Dictionary<String, Object>
            {
                { "x", "1" },
                { "y", "2" }
            };

            var flist = new List<foo>
            {
                new foo() { A = 3, B = "e", AList = new List<String>() { "E", "F", "G" }, ADic = bdic }
            };

            var dic = new Dictionary<String, Object>
            {
                { "x", "1" },
                { "y", "2" }
            };


            var entity = new foo()
            {
                A = 1,
                B = "2",
                C = DateTime.Now,
                AList = new List<String>() { "A", "B", "C" },
                BList = flist,
                CList = new List<String>() { "A1", "B1", "C1" },
                ADic = dic,
                BDic = bdic
            };

            var json = entity.ToJson();

            var fentity = json.ToJsonEntity(typeof(foo));
        }
    }

    class foo
    {
        public Int32 A { get; set; }

        public String B { get; set; }

        public DateTime C { get; set; }

        public IList<String> AList { get; set; }

        public IList<foo> BList { get; set; }

        public List<String> CList { get; set; }

        public Dictionary<String, Object> ADic { get; set; }

        public IDictionary<String, Object> BDic { get; set; }
    }
}
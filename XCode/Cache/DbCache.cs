using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using NewLife.Collections;
using NewLife.Log;
using NewLife.Serialization;
using NewLife.Threading;
using XCode;
using XCode.Configuration;
using XCode.DataAccessLayer;
using XCode.Extension;

namespace NewLife.Caching
{
    /// <summary>数据库缓存。利用数据表来缓存信息</summary>
    /// <remarks>
    /// 构建一个操作队列，新增、更新、删除等操作全部排队单线程执行，以改进性能
    /// </remarks>
    public class DbCache : NewLife.Caching.Cache
    {
        #region 属性

        /// <summary>实体工厂</summary>
        protected IEntityOperate Factory { get; }

        /// <summary>分组字段</summary>
        protected Field GroupField { get; }

        /// <summary>主键字段</summary>
        protected Field KeyField { get; }

        /// <summary>时间字段</summary>
        protected Field TimeField { get; }

        /// <summary>定时清理时间，默认60秒</summary>
        public Int32 Period { get; set; } = 60;

        #endregion

        #region 构造

        /// <summary>实例化一个数据库缓存</summary>
        /// <param name="factory"></param>
        /// <param name="groupName"></param>
        /// <param name="keyName"></param>
        /// <param name="timeName"></param>
        public DbCache(IEntityOperate factory = null, String groupName = null, String keyName = null, String timeName = null)
        {
            if (factory == null) factory = MyDbCache.Meta.Factory;
            if (!(factory.Default is IDbCache)) throw new XCodeException("实体类[{0}]需要实现[{1}]接口", factory.EntityType.FullName, typeof(IDbCache).FullName);

            var name = factory.EntityType.Name;
            if (!groupName.IsNullOrWhiteSpace())
            {
                GroupField = factory.Table.FindByName(groupName);
            }
            var key = !keyName.IsNullOrEmpty() ? factory.Table.FindByName(keyName) : factory.Unique;
            if (key == null || key.Type != typeof(String)) throw new XCodeException("[{0}]没有字符串类型的主键".F(name));

            TimeField = (!timeName.IsNullOrEmpty() ? factory.Table.FindByName(timeName) : factory.MasterTime) as Field;

            Factory  = factory;
            KeyField = key as Field;
            Name     = name;

            // 关闭日志
            var db = factory.Session.Dal.Db;
            db.ShowSQL                  =  false;
            (db as DbBase).TraceSQLTime *= 10;

            Init(null);
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            clearTimer.TryDispose();
            clearTimer = null;
        }

        #endregion

        #region 属性

        /// <summary>缓存个数。高频使用时注意性能</summary>
        public override Int32 Count => Factory.Count;

        /// <summary>所有键。实际返回只读列表新实例，数据量较大时注意性能</summary>
        public override ICollection<String> Keys => Factory.FindAll().Select(e => e[Factory.Unique] as String).ToList();

        #endregion

        #region 方法

        /// <summary>初始化配置</summary>
        /// <param name="config"></param>
        public override void Init(String config)
        {
            if (clearTimer == null)
            {
                var period = Period;
                clearTimer = new TimerX(RemoveNotAlive, null, period * 1000, period * 1000)
                {
                    Async      = true,
                    CanExecute = () => Count > 0
                };
            }
        }

        private DictionaryCache<String, IDbCache> _cache = new DictionaryCache<String, IDbCache>()
        {
            Expire    = 60,
            AllowNull = false,
        };

        private IDbCache Find(String key = null, String group = null)
        {
            if (key.IsNullOrEmpty() && group.IsNullOrEmpty()) return null;
            string cacheKey = GetCacheKey(key, group);
            if (_cache.FindMethod == null)
            {
                _cache.FindMethod = CacheFindMethod;
            }
            return _cache[cacheKey];
        }

        private IDbCache CacheFindMethod(string cacheKey)
        {
            Match match = Regex.Match(cacheKey, "(?<group>.*?)##(?<key>.+)");
            var groupName = match.Groups["group"].Value;
            var keyName = match.Groups["key"];
            if ((object) GroupField == null)
            {
                return Factory.Find(KeyField == keyName) as IDbCache;
            }
            return Factory.Find(GroupField == groupName & KeyField == keyName) as IDbCache;
        }

        private IDbCache FindRealKey(String cacheKey)
        {
            if (cacheKey.IsNullOrEmpty()) return null;
            if (_cache.FindMethod == null)
            {
                _cache.FindMethod = CacheFindMethod;
            }
            return _cache[cacheKey];
        }

        private IList<IDbCache> FindAll(String group, params String[] keys)
        {
            if ((object) GroupField == null) return new List<IDbCache>();
            if (group.IsNullOrEmpty()) return new List<IDbCache>();
            var exp = new WhereExpression();
            exp &= GroupField == @group;
            if (keys?.Length > 0)
            {
                exp &= KeyField.In(keys);
            }
            IList<IEntity> findAll = Factory.FindAll(exp, null, null, 0, 0);
            return findAll.AsParallel().Select(r => r as IDbCache).Where(r => r != null).ToList();
        }

        #endregion

        private string GetCacheKey(IDbCache cache)
        {
            return GetCacheKey(cache.Name, cache.Group);
        }

        private string GetCacheKey(String key = null, String group = null)
        {
            return $"{group}##{key}";
        }

        #region 基本操作

        /// <summary>是否包含缓存项</summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public override Boolean ContainsKey(String key) => Find(key) != null;

        /// <summary>
        /// 是否包含分组子对象
        /// </summary>
        /// <param name="group"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public Boolean ContainsGroupAndKey(String group, String key) => Find(key, group) != null;

        /// <summary>
        /// 是否包含分组
        /// </summary>
        /// <param name="group"></param>
        /// <returns></returns>
        public Boolean ContainsGroup(String group)
        {
            if ((object) GroupField                       == null) return false;
            return Factory.FindCount(GroupField == group) > 0;
        }

        /// <summary>添加缓存项，已存在时更新</summary>
        /// <typeparam name="T">值类型</typeparam>
        /// <param name="key">键</param>
        /// <param name="value">值</param>
        /// <param name="expire">过期时间，秒。小于0时采用默认缓存时间</param>
        /// <returns></returns>
        public override Boolean Set<T>(String key, T value, Int32 expire = -1)
        {
            if (expire < 0) expire = Expire;
            var e = Find(key);
            if (e == null)
            {
                //e = Factory.GetOrAdd(key) as IDbCache;
                e      = Factory.Create() as IDbCache;
                e.Name = key;
                if (e != null) _cache[GetCacheKey(e)] = e;
            }
            e.Value       = value.ToJson();
            e.ExpiredTime = TimerX.Now.AddSeconds(expire);

            if (e.CreateTime.Year < 2000) e.CreateTime = TimerX.Now;
            e.SaveAsync();

            return true;
        }
        /// <summary>
        /// 设置分组中指定Key的值
        /// </summary>
        /// <typeparam name="T">值类型</typeparam>
        /// <param name="group">分组</param>
        /// <param name="key">键</param>
        /// <param name="value">值</param>
        /// <param name="expire">过期时间</param>
        /// <returns></returns>
        public Boolean Set<T>(String group, String key, T value, Int32 expire = -1)
        {
            if (expire < 0) expire = Expire;
            var e = Find(key, group);
            if (e == null)
            {
                //e = Factory.GetOrAdd(key) as IDbCache;
                e       = Factory.Create() as IDbCache;
                e.Group = group;
                e.Name  = key;
                if (e != null) _cache[GetCacheKey(e)] = e;
            }
            e.Value       = value.ToJson();
            e.ExpiredTime = TimerX.Now.AddSeconds(expire);

            if (e.CreateTime.Year < 2000) e.CreateTime = TimerX.Now;
            e.SaveAsync();

            return true;
        }
        /// <summary>
        /// 设置分组中指定Key的值
        /// </summary>
        /// <typeparam name="T">值类型</typeparam>
        /// <param name="group">分组</param>
        /// <param name="key">键</param>
        /// <param name="value">值</param>
        /// <param name="expire">过期时间</param>
        /// <returns></returns>
        public Boolean Set<T>(String group, String key, T value, TimeSpan expire) => Set(group, key, value, (Int32) expire.TotalSeconds);
        /// <summary>
        /// 设置分组缓存
        /// </summary>
        /// <typeparam name="T">值类型</typeparam>
        /// <param name="group">分组</param>
        /// <param name="list">值列表</param>
        /// <param name="keyFunc">根据值类型取键函数</param>
        /// <param name="expire">过期时间</param>
        /// <returns></returns>
        public Boolean SetGroup<T>(String group, IList<T> list, Func<T, object> keyFunc, Int32 expire = -1)
        {
            if (expire < 0) expire = Expire;
            list.GroupBy(keyFunc).ToList().ForEach(value => { Set(@group, value.Key.ToString(), value.ToList(), expire); });
            return true;
        }
        /// <summary>
        /// 设置分组缓存
        /// </summary>
        /// <typeparam name="T">值类型</typeparam>
        /// <param name="group">分组</param>
        /// <param name="list">值列表</param>
        /// <param name="keyFunc">根据值类型取键函数</param>
        /// <param name="expire">过期时间</param>
        /// <returns></returns>
        public Boolean SetGroup<T>(String group, IList<T> list, Func<T, object> keyFunc, TimeSpan expire) => SetGroup(group, list, keyFunc, (Int32) expire.TotalSeconds);

        /// <summary>获取缓存项，不存在时返回默认值</summary>
        /// <param name="key">键</param>
        /// <returns></returns>
        public override T Get<T>(String key)
        {
            var e = Find(key);
            if (e == null) return default(T);

            var value = e.Value;
            //return JsonHelper.Convert<T>(value);
            //if (typeof(T) == typeof(Byte[])) return (T)(Object)(value + "").ToBase64();
            if (typeof(T) == typeof(String)) return (T) (Object) value;

            //return value.ChangeType<T>();
            return value.ToJsonEntity<T>();
        }
        /// <summary>获取缓存项，不存在时返回默认值</summary>
        /// <param name="key">键</param>
        /// <param name="group">分组</param>
        /// <returns></returns>
        public T Get<T>(String key, String group)
        {
            var e = Find(key, group);
            if (e == null) return default(T);

            var value = e.Value;
            //return JsonHelper.Convert<T>(value);
            //if (typeof(T) == typeof(Byte[])) return (T)(Object)(value + "").ToBase64();
            if (typeof(T) == typeof(String)) return (T) (Object) value;

            //return value.ChangeType<T>();
            return value.ToJsonEntity<T>();
        }
        /// <summary>
        /// 获取分组缓存
        /// </summary>
        /// <typeparam name="T">值类型</typeparam>
        /// <param name="group">分组</param>
        /// <param name="keys">键列表</param>
        /// <returns></returns>
        public IList<T> GetGroup<T>(String group, params String[] keys)
        {
            List<T> list = new List<T>();
            var e = FindAll(group, keys);
            //先拿出来 避免并发
            if (true == _cache.Keys?.Any())
            {
                var tmp = _cache.Keys?.Where(r => r.StartsWith(GetCacheKey(group: @group)));
                if (keys?.Length > 0)
                {
                    List<string> cacheKeys = keys.Select(r => GetCacheKey(group, r)).ToList();
                    if (cacheKeys.Any())
                    {
                        tmp = tmp.Where(r => cacheKeys.Contains(r));
                    }
                }
                var cacheDatas = tmp.Select(FindRealKey).Where(r => r != null).ToList();
                cacheDatas.Where(r => true == e?.All(i => i?.Name != r?.Name)).ToList().ForEach(r => e?.Add(r));
            }
            if (!e.Any()) return new List<T>();
            List<IGrouping<String, IDbCache>> groupBy = e.Where(r => !r.Value.IsNullOrWhiteSpace()).GroupBy(r => r.Name).ToList();
            foreach (IGrouping<String, IDbCache> grouping in groupBy)
            {
                var groupList = new List<T>();
                foreach (IDbCache dbCache in grouping)
                {
                    if (typeof(T) == typeof(String))
                    {
                        groupList.Add((T) (Object) dbCache.Value);
                    }
                    else
                    {
                        try
                        {
                            var decode = dbCache.Value.ToJsonEntity<object>();
                            if (decode is IDictionary)
                            {
                                groupList.Add(dbCache.Value.ToJsonEntity<T>());
                            }
                            else if (decode is IList)
                            {
                                groupList.AddRange(dbCache.Value.ToJsonEntity<List<T>>());
                            }
                        }
                        catch (Exception ex)
                        {
                            XTrace.WriteException(ex);
                        }
                    }
                }
                if (groupList.Any())
                {
                    list.AddRange(groupList);
                }
                if (grouping.Count() > 1)
                {
                    DateTime expired = grouping.Max(r => r.ExpiredTime);
                    Task.Run(() =>
                    {
                        RemoveGroup(group);
                        SetGroup(group, groupList, arg => grouping.Key, expired - DateTime.Now);
                    });
                }
            }
            return list;
        }

        /// <summary>批量移除缓存项</summary>
        /// <param name="keys">键集合</param>
        /// <returns>实际移除个数</returns>
        public override Int32 Remove(params String[] keys)
        {
            if (Count == 0) return 0;
            var count = 0;
            foreach (var item in keys)
            {
                var cacheKey = GetCacheKey(item);
                if (_cache.ContainsKey(cacheKey))
                {
                    _cache.Remove(cacheKey);
                    count++;
                }
            }
            IList<IEntity> findAll = Factory.FindAll(KeyField.In(keys), null, null, 0, 0);
            if (findAll.Any())
                count = findAll.Delete();
            return count;
            //var list = Factory.FindAll(KeyField.In(keys), null, null, 0, 0);
            //foreach (IDbCache item in list)
            //{
            //    _cache.Remove(item.Name);
            //}
            //return list.Delete();
        }
        /// <summary>批量移除缓存项</summary>
        /// <param name="group">分组</param>
        /// <param name="keys">键集合</param>
        /// <returns>实际移除个数</returns>
        public Int32 Remove(string group, params String[] keys)
        {
            if (Count == 0) return 0;
            var count = 0;
            foreach (var item in keys)
            {
                var cacheKey = GetCacheKey(item, group);
                if (_cache.ContainsKey(cacheKey))
                {
                    _cache.Remove(cacheKey);
                    count++;
                }
            }
            IList<IEntity> findAll = Factory.FindAll(GroupField == @group & KeyField.In(keys), null, null, 0, 0);
            if (findAll.Any())
                count = findAll.Delete();
            return count;
        }
        /// <summary>
        /// 移除分组缓存
        /// </summary>
        /// <param name="group">分组</param>
        /// <returns></returns>
        public Int32 RemoveGroup(string group)
        {
            if (Count == 0) return 0;
            var count = 0;
            _cache.Keys.Where(r => r.StartsWith(GetCacheKey(group: group))).ToList().ForEach(cacheKey => _cache.Remove(cacheKey));
            IList<IEntity> findAll = Factory.FindAll(GroupField == @group, null, null, 0, 0);
            count = findAll.Delete();
            return count;
        }

        /// <summary>删除所有配置项</summary>
        public override void Clear()
        {
            Factory.Session.Truncate();
            _cache.Clear();
        }

        /// <summary>设置缓存项有效期</summary>
        /// <param name="key">键</param>
        /// <param name="expire">过期时间</param>
        /// <returns>设置是否成功</returns>
        public override Boolean SetExpire(String key, TimeSpan expire)
        {
            var e = Find(key);
            if (e == null) return false;

            e.ExpiredTime = TimerX.Now.Add(expire);
            e.SaveAsync();

            return true;
        }
        /// <summary>设置缓存项有效期</summary>
        /// <param name="group">分组</param>
        /// <param name="key">键</param>
        /// <param name="expire">过期时间</param>
        /// <returns>设置是否成功</returns>
        public Boolean SetExpire(String group, String key, TimeSpan expire)
        {
            var e = Find(key, group);
            if (e == null) return false;

            e.ExpiredTime = TimerX.Now.Add(expire);
            e.SaveAsync();

            return true;
        }
        /// <summary>设置分组所有缓存项有效期</summary>
        /// <param name="group">分组</param>
        /// <param name="expire">过期时间</param>
        /// <returns>设置是否成功</returns>
        public Boolean SetGroupAllExpire(String group, TimeSpan expire)
        {
            var e = FindAll(group);
            if (!e.Any()) return false;
            foreach (var cache in e)
            {
                if (!cache.Name.IsNullOrWhiteSpace())
                {
                    SetExpire(group, cache.Name, expire);
                }
            }
            return true;
        }

        /// <summary>获取缓存项有效期，不存在时返回Zero</summary>
        /// <param name="key">键</param>
        /// <returns></returns>
        public override TimeSpan GetExpire(String key)
        {
            var e = Find(key);
            if (e == null) return TimeSpan.Zero;

            return e.ExpiredTime - TimerX.Now;
        }
        /// <summary>获取缓存项有效期，不存在时返回Zero</summary>
        /// <param name="key">键</param>
        /// <param name="group">分组</param>
        /// <returns></returns>
        public TimeSpan GetExpire(String key, String group)
        {
            var e = Find(key, group);
            if (e == null) return TimeSpan.Zero;

            return e.ExpiredTime - TimerX.Now;
        }

        #endregion

        #region 高级操作

        /// <summary>添加，已存在时不更新，常用于锁争夺</summary>
        /// <typeparam name="T">值类型</typeparam>
        /// <param name="key">键</param>
        /// <param name="value">值</param>
        /// <param name="expire">过期时间，秒。小于0时采用默认缓存时间</param>
        /// <returns></returns>
        public override Boolean Add<T>(String key, T value, Int32 expire = -1)
        {
            if (expire < 0) expire = Expire;
            var e = Find(key);
            if (e != null) return false;

            e             = Factory.Create() as IDbCache;
            e.Name        = key;
            e.Value       = value.ToJson();
            e.ExpiredTime = TimerX.Now.AddSeconds(expire);
            (e as IEntity).Insert();

            _cache[GetCacheKey(e)] = e;

            return true;
        }

        public Boolean Add<T>(String key, T value, TimeSpan expire) => Add(key, value, (Int32) expire.TotalSeconds);

        public Boolean Add<T>(String group, String key, T value, Int32 expire = -1)
        {
            if (expire < 0) expire = Expire;
            var e = Find(key, group);
            if (e != null) return false;

            e             = Factory.Create() as IDbCache;
            e.Group       = group;
            e.Name        = key;
            e.Value       = value.ToJson();
            e.ExpiredTime = TimerX.Now.AddSeconds(expire);
            (e as IEntity).Insert();

            _cache[GetCacheKey(e)] = e;

            return true;
        }

        public Boolean Add<T>(String group, String key, T value, TimeSpan expire) => Add(group, key, value, (Int32) expire.TotalSeconds);

        public Boolean AddGroup<T>(String group, IList<T> list, Func<T, object> keyFunc, Int32 expire = -1)
        {
            try
            {
                if (expire < 0) expire = Expire;
                list.GroupBy(keyFunc).ToList().ForEach(team =>
                {
                    var key = team.Key.ToString();
                    var cacheKey = GetCacheKey(key, group);
                    var e = FindRealKey(cacheKey);
                    if (e != null)
                    {
                        return;
                    }
                    e                = Factory.Create() as IDbCache;
                    e.Group          = group;
                    e.Name           = key;
                    e.Value          = team.ToList().ToJson();
                    e.ExpiredTime    = TimerX.Now.AddSeconds(expire);
                    _cache[cacheKey] = e;
                    e.SaveAsync();
                });
            }
            catch (Exception ex)
            {
                XTrace.WriteException(ex);
                return false;
            }
            return true;
        }

        public Boolean AddGroup<T>(String group, IList<T> list, Func<T, object> keyFunc, TimeSpan expire) => AddGroup(group, list, keyFunc, (Int32) expire.TotalSeconds);

        #endregion

        #region 清理过期缓存

        /// <summary>清理会话计时器</summary>
        private TimerX clearTimer;

        /// <summary>移除过期的缓存项</summary>
        void RemoveNotAlive(Object state)
        {
            // 这里先计算，性能很重要
            var now = TimerX.Now;
            var list = Factory.FindAll(TimeField < now, null, null, 0, 0);
            foreach (IDbCache item in list)
            {
                var cacheKey = GetCacheKey(item);
                _cache.Remove(cacheKey);
            }
            list.Delete();
        }

        #endregion

        #region 性能测试

        /// <summary>使用指定线程测试指定次数</summary>
        /// <param name="times">次数</param>
        /// <param name="threads">线程</param>
        /// <param name="rand">随机读写</param>
        /// <param name="batch">批量操作</param>
        public override void BenchOne(Int64 times, Int32 threads, Boolean rand, Int32 batch)
        {
            if (rand)
                times *= 1;
            else
                times *= 1000;

            base.BenchOne(times, threads, rand, batch);
        }

        #endregion
    }

    /// <summary>数据缓存接口</summary>
    public interface IDbCache
    {
        /// <summary>分组</summary>
        String Group { get; set; }

        /// <summary>名称</summary>
        String Name { get; set; }

        /// <summary>键值</summary>
        String Value { get; set; }

        /// <summary>创建时间</summary>
        DateTime CreateTime { get; set; }

        /// <summary>过期时间</summary>
        DateTime ExpiredTime { get; set; }

        /// <summary>异步保存</summary>
        /// <param name="msDelay"></param>
        /// <returns></returns>
        Boolean SaveAsync(Int32 msDelay = 0);
    }
}
using System;
using System.Collections.Generic;
using System.ComponentModel;
using XCode;
using XCode.Configuration;
using XCode.DataAccessLayer;

namespace XCode.Extension
{
    /// <summary>数据缓存</summary>
    [Serializable]
    [DataObject]
    [Description("数据缓存")]
    [BindIndex("IU_TableCache_Id", true, "Id")]
    [BindIndex("IX_MyDbCache_Group", false, "Group")]
    [BindIndex("IX_MyDbCache_Name", false, "Name")]
    [BindIndex("IX_MyDbCache_ExpiredTime", false, "ExpiredTime")]
    [BindTable("MyDbCache", Description = "数据缓存", ConnName = "DbCache", DbType = DatabaseType.None)]
    public partial class MyDbCache : IMyDbCache
    {
        #region 属性
        private Int64 _Id;
        /// <summary>Id标识</summary>
        [DisplayName("Id标识")]
        [Description("Id标识")]
        [DataObjectField(true, true, false, 0)]
        [BindColumn("Id", "Id标识", "")]
        public Int64 Id { get { return _Id; } set { if (OnPropertyChanging(__.Id, value)) { _Id = value; OnPropertyChanged(__.Id); } } }

        private String _Group;
        /// <summary>分组</summary>
        [DisplayName("分组")]
        [Description("分组")]
        [DataObjectField(false, false, false)]
        [BindColumn("Group", "分组", "")]
        public String Group { get { return _Group; } set { if (OnPropertyChanging(__.Group, value)) { _Group = value; OnPropertyChanged(__.Group); } } }

        private String _Name;
        /// <summary>名称</summary>
        [DisplayName("名称")]
        [Description("名称")]
        [DataObjectField(false, false, false)]
        [BindColumn("Name", "名称", "", Master = true)]
        public String Name { get { return _Name; } set { if (OnPropertyChanging(__.Name, value)) { _Name = value; OnPropertyChanged(__.Name); } } }

        private String _Value;
        /// <summary>键值</summary>
        [DisplayName("键值")]
        [Description("键值")]
        [DataObjectField(false, false, false)]
        [BindColumn("Value", "键值", "")]
        public String Value { get { return _Value; } set { if (OnPropertyChanging(__.Value, value)) { _Value = value; OnPropertyChanged(__.Value); } } }

        private DateTime _CreateTime;
        /// <summary>创建时间</summary>
        [DisplayName("创建时间")]
        [Description("创建时间")]
        [DataObjectField(false, false, true, 0)]
        [BindColumn("CreateTime", "创建时间", "")]
        public DateTime CreateTime { get { return _CreateTime; } set { if (OnPropertyChanging(__.CreateTime, value)) { _CreateTime = value; OnPropertyChanged(__.CreateTime); } } }

        private DateTime _ExpiredTime;
        /// <summary>过期时间</summary>
        [DisplayName("过期时间")]
        [Description("过期时间")]
        [DataObjectField(false, false, true, 0)]
        [BindColumn("ExpiredTime", "过期时间", "")]
        public DateTime ExpiredTime { get { return _ExpiredTime; } set { if (OnPropertyChanging(__.ExpiredTime, value)) { _ExpiredTime = value; OnPropertyChanged(__.ExpiredTime); } } }
        #endregion

        #region 获取/设置 字段值
        /// <summary>获取/设置 字段值</summary>
        /// <param name="name">字段名</param>
        /// <returns></returns>
        public override Object this[String name]
        {
            get
            {
                switch (name)
                {
                    case __.Id: return _Id;
                    case __.Group: return _Group;
                    case __.Name: return _Name;
                    case __.Value: return _Value;
                    case __.CreateTime: return _CreateTime;
                    case __.ExpiredTime: return _ExpiredTime;
                    default: return base[name];
                }
            }
            set
            {
                switch (name)
                {
                    case __.Id: _Id = value.ToLong(); break;
                    case __.Group: _Group = Convert.ToString(value); break;
                    case __.Name: _Name = Convert.ToString(value); break;
                    case __.Value: _Value = Convert.ToString(value); break;
                    case __.CreateTime: _CreateTime = value.ToDateTime(); break;
                    case __.ExpiredTime: _ExpiredTime = value.ToDateTime(); break;
                    default: base[name] = value; break;
                }
            }
        }
        #endregion

        #region 字段名
        /// <summary>取得数据缓存字段信息的快捷方式</summary>
        public partial class _
        {
            /// <summary>Id标识</summary>
            public static readonly Field Id = FindByName(__.Id);

            /// <summary>分组</summary>
            public static readonly Field Group = FindByName(__.Group);

            /// <summary>名称</summary>
            public static readonly Field Name = FindByName(__.Name);

            /// <summary>键值</summary>
            public static readonly Field Value = FindByName(__.Value);

            /// <summary>创建时间</summary>
            public static readonly Field CreateTime = FindByName(__.CreateTime);

            /// <summary>过期时间</summary>
            public static readonly Field ExpiredTime = FindByName(__.ExpiredTime);

            static Field FindByName(String name) { return Meta.Table.FindByName(name); }
        }

        /// <summary>取得数据缓存字段名称的快捷方式</summary>
        public partial class __
        {
            /// <summary>Id标识</summary>
            public const String Id = "Id";

            /// <summary>分组</summary>
            public const String Group = "Group";

            /// <summary>名称</summary>
            public const String Name = "Name";

            /// <summary>键值</summary>
            public const String Value = "Value";

            /// <summary>创建时间</summary>
            public const String CreateTime = "CreateTime";

            /// <summary>过期时间</summary>
            public const String ExpiredTime = "ExpiredTime";
        }
        #endregion
    }

    /// <summary>数据缓存接口</summary>
    public partial interface IMyDbCache
    {
        #region 属性
        /// <summary>Id标识</summary>
        Int64 Id { get; set; }

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
        #endregion

        #region 获取/设置 字段值
        /// <summary>获取/设置 字段值</summary>
        /// <param name="name">字段名</param>
        /// <returns></returns>
        Object this[String name] { get; set; }
        #endregion
    }
}
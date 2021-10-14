using Dapper;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;

namespace CreateTableByLeetCode
{
    class Program
    {
        static void Main(string[] args)
        {
            CreateTableAndInsert();
        }

        public static SqlDbType GetDbType(Type type)
        {
            if (type.Equals(typeof(Int64)))
                return SqlDbType.BigInt;
            if (type.Equals(typeof(Boolean)))
                return SqlDbType.Bit;
            if (type.Equals(typeof(Decimal)))
                return SqlDbType.Decimal;
            if (type.Equals(typeof(String)))
                return SqlDbType.VarChar;
            else
                throw new Exception("暂无此类型");
            //switch (type)
            //{
            //    case Int64:
            //        return SqlDbType.BigInt;
            //    case SqlDbType.Binary:
            //        return typeof(Object);
            //    case SqlDbType.Bit:
            //        return typeof(Boolean);
            //    case SqlDbType.Char:
            //        return typeof(String);
            //    case SqlDbType.DateTime:
            //        return typeof(DateTime);
            //    case SqlDbType.Decimal:
            //        return typeof(Decimal);
            //    case SqlDbType.Float:
            //        return typeof(Double);
            //    case SqlDbType.Image:
            //        return typeof(Object);
            //    case SqlDbType.Int:
            //        return typeof(Int32);
            //    case SqlDbType.Money:
            //        return typeof(Decimal);
            //    case SqlDbType.NChar:
            //        return typeof(String);
            //    case SqlDbType.NText:
            //        return typeof(String);
            //    case SqlDbType.NVarChar:
            //        return typeof(String);
            //    case SqlDbType.Real:
            //        return typeof(Single);
            //    case SqlDbType.SmallDateTime:
            //        return typeof(DateTime);
            //    case SqlDbType.SmallInt:
            //        return typeof(Int16);
            //    case SqlDbType.SmallMoney:
            //        return typeof(Decimal);
            //    case SqlDbType.Text:
            //        return typeof(String);
            //    case SqlDbType.Timestamp:
            //        return typeof(Object);
            //    case SqlDbType.TinyInt:
            //        return typeof(Byte);
            //    case SqlDbType.Udt://自定义的数据类型
            //        return typeof(Object);
            //    case SqlDbType.UniqueIdentifier:
            //        return typeof(Object);
            //    case SqlDbType.VarBinary:
            //        return typeof(Object);
            //    case SqlDbType.VarChar:
            //        return typeof(String);
            //    case SqlDbType.Variant:
            //        return typeof(Object);
            //    case SqlDbType.Xml:
            //        return typeof(Object);
            //    default:
            //        return null;
            //}
        }

        static void CreateTableAndInsert()
        {
            var dt = LoadJsonFile();
            CreateTable(dt);
            InsertData(dt);
        }

        static void CreateTable(DataTable dt)
        {
            IDbConnection db = GetConn();

            string sql = $"create table {dt.TableName} (";
            foreach(DataColumn f in dt.Columns)
            {
                sql += $"{f.ColumnName} {GetDbType(f.DataType)} {(GetDbType(f.DataType).Equals(SqlDbType.VarChar)?"(100)":"")},";
            }
            sql=sql.Substring(0, sql.Length - 1);
            sql += ")";
            
            db.Execute(sql);
            Console.WriteLine("创建表成功");
        }

        static void InsertData(DataTable dt)
        {
            IDbConnection conn = GetConn();
            
            string sql = $"insert into {dt.TableName} values(";
            foreach(DataColumn dc in dt.Columns)
            {
                sql += $"@{dc.ColumnName},";
            }
            sql = sql.Substring(0, sql.Length - 1);
            sql += ")";
            using (conn)
            {
                var adapter = new SqlDataAdapter();
                adapter.InsertCommand = new SqlCommand(sql, (SqlConnection)conn);
                for(int i = 0; i < dt.Columns.Count; i++)
                {
                    adapter.InsertCommand.Parameters.Add($"@{dt.Columns[i].ColumnName}", GetDbType(dt.Columns[i].DataType),50,$"{dt.Columns[i].ColumnName}");
                }
                adapter.Update(dt);
                Console.WriteLine("数据插入成功");
            }
        }

        static IDbConnection GetConn()
        {
            string connStr = "Data Source=LINYIXIN;Initial Catalog=LeetCodeDataBase;Integrated Security=True";
            return new SqlConnection(connStr);
        }

        static DataTable LoadJsonFile()
        {
            string path = "D:/code/CreateTableByLeetCode/data.json";
            using (System.IO.StreamReader file = System.IO.File.OpenText(path))
            {
                using (JsonTextReader reader = new JsonTextReader(file))
                {
                    var o = JToken.ReadFrom(reader);
                    var headers = o["headers"];

                    //获取表名
                    var tableName = (headers as JObject).Properties().ToList()[0].Name;

                    //获取字段名
                    var fields = headers.Values().ElementAt(0).Select(c => (string)c).ToList();

                    var type = o["type"][tableName].Values().Select(c=>(string)c).ToList();

                    //读取数据
                    var data = o["rows"][tableName].ToArray();



                    DataTable dt = new DataTable();
                    dt.TableName = tableName;


                    //添加列
                    for(int i = 0; i < fields.Count(); i++)
                    {
                        dt.Columns.Add(fields[i], Type.GetType(type[i]));
                    }

                    //添加行
                    foreach(var d in data)
                    {
                        DataRow dr = dt.NewRow();
                        for(int i = 0; i < fields.Count(); i++)
                        {
                            dr[fields[i]] = d[i];
                        }
                        dt.Rows.Add(dr);
                    }

                    return dt;
                }
            }
        }
    }
}

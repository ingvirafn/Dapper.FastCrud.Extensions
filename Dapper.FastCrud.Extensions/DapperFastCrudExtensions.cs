using Dapper;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Web;

namespace Dapper.FastCrud
{
    public static class DapperFastCrudExtensions
    {
        public static IEnumerable<T> GetBy<T, TProperty>(
            this SqlConnection connection
            , Expression<Func<T, TProperty>> property,
            TProperty filter
            )
        {
            var sqlBuilder = Dapper.FastCrud.OrmConfiguration.GetSqlBuilder<T>();
            var cols = sqlBuilder.ConstructColumnEnumerationForSelect();
            var propertyName = sqlBuilder.GetColumnName(property);
            var sqlQuery = $"select {cols} from {sqlBuilder.GetTableName()} where {propertyName} = @f";
            DynamicParameters dp = new DynamicParameters();
            dp.Add("f", filter);
            return connection.Query<T>(sqlQuery, param: dp);
        }
        public static IEnumerable<T> Like<T>(
            this SqlConnection connection
            , Expression<Func<T, string>> property,
            string filter
            )
        {
            var sqlBuilder = Dapper.FastCrud.OrmConfiguration.GetSqlBuilder<T>();
            var cols = sqlBuilder.ConstructColumnEnumerationForSelect();
            var propertyName = sqlBuilder.GetColumnName(property);
            var sqlQuery = $"select {cols} from {sqlBuilder.GetTableName()} where {propertyName} like '%' + @f + '%'";
            DynamicParameters dp = new DynamicParameters();
            dp.Add("f", filter);
            return connection.Query<T>(sqlQuery, param: dp);
        }

        public static SqlConnection CreateTable<TEntity>(this SqlConnection conn, bool dropFirst)
        {
            if (dropFirst)
                conn.DropTable<TEntity>();
            return conn.CreateTable<TEntity>();
        }

        public static SqlConnection CreateTable<TEntity>(this SqlConnection conn)
        {
            if (!conn.TableExists<TEntity>())
            {
                var createScript = GenerateCreateScript<TEntity>();
                conn.Execute(createScript);
            }
            return conn;
        }

        public static IEnumerable<string> GetTableList(this SqlConnection conn)
        {
            string tableListQry = @"SELECT O.NAME FROM SYS.ALL_OBJECTS O WHERE O.TYPE = 'U' AND O.IS_MS_SHIPPED = 0";
            return conn.Query<string>(tableListQry);
        }

        public static bool TableExists<TEntity>(this SqlConnection conn)
        {
            var tableName = GetTableName<TEntity>();
            string checkTableScript = @"IF OBJECT_ID('{0}.{1}') IS NOT NULL BEGIN SELECT CAST(1 AS BIT) END ELSE BEGIN SELECT CAST(0 AS BIT) END";
            string scr = string.Format(checkTableScript, tableName.Item1, tableName.Item2);
            var res = conn.Query<bool>(scr).Single();
            return res;
        }
        private static string GenerateCreateScript<TEntity>()
        {
            var props = GetProps<TEntity>();
            var tableName = GetTableName<TEntity>();
            var baseScript = String.Format("CREATE TABLE {0}.{1}", tableName.Item1, tableName.Item2);
            var columnsScript = String.Join(",", props.Select(x => GetPropertyAsColumnCreate(x)));
            var resultScript = String.Format("{0} ({1})", baseScript, columnsScript);
            return resultScript;
        }

        public static int RowCount<TEntity>(this SqlConnection conn)
        {
            var tableName = GetTableName<TEntity>();
            string checkTableScript = @"SELECT COUNT(*) FROM {0}.{1}";
            string scr = string.Format(checkTableScript, tableName.Item1, tableName.Item2);
            var res = conn.Query<int>(scr).Single();
            return res;
        }

        private static void DropTable<TEntity>(this SqlConnection conn)
        {
            var tableName = GetTableName<TEntity>();
            string dropTableScript = @"IF OBJECT_ID('{0}.{1}') IS NOT NULL BEGIN DROP TABLE {0}.{1} END";
            string scr = string.Format(dropTableScript, tableName.Item1, tableName.Item2);
            conn.Execute(scr);
        }


        private static string GetPropertyAsColumnCreate(PropertyInfo x)
        {
            string colName = x.Name;
            string colType;
            if (x.PropertyType == typeof(int)) { colType = "int not null"; }
            else if (x.PropertyType == typeof(long)) { colType = "bigint not null"; }
            else if (x.PropertyType == typeof(int?)) { colType = "int null"; }
            else if (x.PropertyType == typeof(long?)) { colType = "bigint null"; }
            else if (x.PropertyType == typeof(decimal)) { colType = "numeric(11,8) not null"; }
            else if (x.PropertyType == typeof(decimal?)) { colType = "numeric(11,8) null"; }
            else if (x.PropertyType == typeof(string)) { colType = "varchar(max) null"; }
            else if (x.PropertyType == typeof(DateTime)) { colType = "datetime not null"; }
            else if (x.PropertyType == typeof(bool)) { colType = "bit not null"; }
            else if (x.PropertyType == typeof(bool?)) { colType = "bit null"; }
            else if (x.PropertyType.BaseType == typeof(Enum)) { colType = "int not null"; }
            else { throw new NotSupportedException("column type not supported"); };

            string ext = String.Empty;

            if (colName.Equals("id", StringComparison.InvariantCultureIgnoreCase))
                ext = "identity(1,1) primary key";

            return String.Format("{0} {1} {2}", colName, colType, ext);
        }

        private static Tuple<string, string> GetTableName<TEntity>(string defaultSchema = "dbo")
        {
            var sqlBuilder = Dapper.FastCrud.OrmConfiguration.GetSqlBuilder<TEntity>();
            return new Tuple<string, string>("dbo", sqlBuilder.GetTableName());
            //var tableAttr = (TableAttribute) typeof(TEntity).GetCustomAttributes(typeof(TableAttribute), true).FirstOrDefault();
            //if (tableAttr != null)
            //    return new Tuple<string, string>(tableAttr.Schema ?? defaultSchema, tableAttr.Name);

            //return new Tuple<string, string>(defaultSchema, typeof(TEntity).Name);
        }

        private static IEnumerable<PropertyInfo> GetProps<TEntity>()
        {
            return typeof(TEntity)
                    .GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                    .Where(x => !x.CustomAttributes.OfType<NotMappedAttribute>().Any());
        }


    }
}
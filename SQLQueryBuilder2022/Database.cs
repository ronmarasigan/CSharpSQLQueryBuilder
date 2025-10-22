using System;
using Microsoft.Data.SqlClient;
using System.Data;

namespace SQLQueryBuilder
{
    public class Database
    {
        private readonly string _connectionString;

        public Database()
        {
            string localDBFilePath = @"";//path to your mdf file
            _connectionString = $@"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename={localDBFilePath};Integrated Security=True;";

        }

        // --- Test connection ---
        public bool TestConnection(out string message)
        {
            try
            {
                using (SqlConnection conn = new SqlConnection(_connectionString))
                {
                    conn.Open();
                    message = "Connection successful.";
                    return true;
                }
            }
            catch (Exception ex)
            {
                message = "Connection failed: " + ex.Message;
                return false;
            }
        }

        // --- ExecuteNonQuery ---
        public int ExecuteNonQuery(SqlQueryBuilder builder)
        {
            return Execute<int>(delegate (SqlCommand cmd) { return cmd.ExecuteNonQuery(); }, builder);
        }

        // --- ExecuteScalar ---
        public object ExecuteScalar(SqlQueryBuilder builder)
        {
            return Execute<object>(delegate (SqlCommand cmd) { return cmd.ExecuteScalar(); }, builder);
        }

        // --- ExecuteReader returning DataTable ---
        public DataTable ExecuteReader(SqlQueryBuilder builder)
        {
            return Execute<DataTable>(delegate (SqlCommand cmd)
            {
                using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                {
                    DataTable dt = new DataTable();
                    adapter.Fill(dt);
                    return dt;
                }
            }, builder);
        }

        // --- Core executor that handles connection and parameters ---
        private T Execute<T>(Func<SqlCommand, T> executor, SqlQueryBuilder builder)
        {
            string sql = builder.BuildQuery();
            SqlParameter[] parameters = builder.GetParameters();

            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                using (SqlCommand cmd = new SqlCommand(sql, conn))
                {
                    if (parameters != null && parameters.Length > 0)
                        cmd.Parameters.AddRange(parameters);

                    conn.Open();
                    return executor(cmd);
                }
            }
        }
    }
}

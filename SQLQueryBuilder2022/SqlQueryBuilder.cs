
using Microsoft.Data.SqlClient;
using System.Text;

namespace SQLQueryBuilder
{
    public class SqlQueryBuilder
    {
        private string? _table;
        private List<string> _columns = new List<string>();
        private List<string> _whereConditions = new List<string>();
        private List<SqlParameter> _parameters = new List<SqlParameter>();
        private string? _orderBy;
        private int? _limit;
        private int? _top;
        private bool _isDistinct = false;
        private List<string> _joins = new List<string>();

        private List<string> _groupByColumns = new List<string>();
        private List<string> _havingConditions = new List<string>();

        private Dictionary<string, SqlParameter> _insertValues = new Dictionary<string, SqlParameter>();
        private Dictionary<string, SqlParameter> _updateValues = new Dictionary<string, SqlParameter>();

        private enum QueryType { Select, Insert, Update, Delete }
        private QueryType _queryType = QueryType.Select;

        // Raw SQL support
        private string? _rawSql;
        private List<SqlParameter> _rawParameters = new List<SqlParameter>();

        // --- SELECT ---
        public SqlQueryBuilder Select(params string[] columns)
        {
            _queryType = QueryType.Select;
            _columns.AddRange(columns);
            return this;
        }

        public SqlQueryBuilder Distinct()
        {
            _isDistinct = true;
            return this;
        }

        public SqlQueryBuilder Top(int n)
        {
            _top = n;
            return this;
        }

        public SqlQueryBuilder From(string table)
        {
            _table = table;
            return this;
        }

        // --- WHERE HELPERS ---
        public SqlQueryBuilder Where(string condition, SqlParameter parameter)
        {
            if (_whereConditions.Count == 0)
                _whereConditions.Add(condition);
            else
                _whereConditions.Add("AND " + condition);

            _parameters.Add(parameter);
            return this;
        }

        public SqlQueryBuilder OrWhere(string condition, SqlParameter parameter)
        {
            if (_whereConditions.Count == 0)
                _whereConditions.Add(condition);
            else
                _whereConditions.Add("OR " + condition);

            _parameters.Add(parameter);
            return this;
        }

        public SqlQueryBuilder NotWhere(string condition, SqlParameter parameter)
        {
            if (_whereConditions.Count == 0)
                _whereConditions.Add("NOT (" + condition + ")");
            else
                _whereConditions.Add("AND NOT (" + condition + ")");

            _parameters.Add(parameter);
            return this;
        }

        public SqlQueryBuilder OrNotWhere(string condition, SqlParameter parameter)
        {
            if (_whereConditions.Count == 0)
                _whereConditions.Add("NOT (" + condition + ")");
            else
                _whereConditions.Add("OR NOT (" + condition + ")");

            _parameters.Add(parameter);
            return this;
        }

        // --- LIKE HELPERS ---
        public SqlQueryBuilder WhereLike(string column, SqlParameter parameter)
        {
            if (_whereConditions.Count == 0)
                _whereConditions.Add($"{column} LIKE {parameter.ParameterName}");
            else
                _whereConditions.Add($"AND {column} LIKE {parameter.ParameterName}");

            _parameters.Add(parameter);
            return this;
        }

        public SqlQueryBuilder OrWhereLike(string column, SqlParameter parameter)
        {
            if (_whereConditions.Count == 0)
                _whereConditions.Add($"{column} LIKE {parameter.ParameterName}");
            else
                _whereConditions.Add($"OR {column} LIKE {parameter.ParameterName}");

            _parameters.Add(parameter);
            return this;
        }

        public SqlQueryBuilder NotWhereLike(string column, SqlParameter parameter)
        {
            if (_whereConditions.Count == 0)
                _whereConditions.Add($"{column} NOT LIKE {parameter.ParameterName}");
            else
                _whereConditions.Add($"AND {column} NOT LIKE {parameter.ParameterName}");

            _parameters.Add(parameter);
            return this;
        }

        public SqlQueryBuilder OrNotWhereLike(string column, SqlParameter parameter)
        {
            if (_whereConditions.Count == 0)
                _whereConditions.Add($"{column} NOT LIKE {parameter.ParameterName}");
            else
                _whereConditions.Add($"OR {column} NOT LIKE {parameter.ParameterName}");

            _parameters.Add(parameter);
            return this;
        }

        // --- IN HELPERS ---
        public SqlQueryBuilder WhereIn(string column, IEnumerable<object> values)
        {
            return AddInCondition(column, values, false, "AND");
        }

        public SqlQueryBuilder NotWhereIn(string column, IEnumerable<object> values)
        {
            return AddInCondition(column, values, true, "AND");
        }

        public SqlQueryBuilder OrWhereIn(string column, IEnumerable<object> values)
        {
            return AddInCondition(column, values, false, "OR");
        }

        public SqlQueryBuilder OrNotWhereIn(string column, IEnumerable<object> values)
        {
            return AddInCondition(column, values, true, "OR");
        }

        // --- Private helper to reduce code duplication ---
        private SqlQueryBuilder AddInCondition(string column, IEnumerable<object> values, bool notIn, string connector)
        {
            if (values == null || !values.Any())
                throw new ArgumentException("Values for IN clause cannot be null or empty");

            var paramNames = new List<string>();
            int i = _parameters.Count; // use count to avoid duplicate param names

            foreach (var val in values)
            {
                string paramName = $"@{column}{i}";
                paramNames.Add(paramName);
                _parameters.Add(new SqlParameter(paramName, val));
                i++;
            }

            string inClause = $"{column} {(notIn ? "NOT IN" : "IN")} ({string.Join(", ", paramNames)})";

            if (_whereConditions.Count > 0)
                _whereConditions.Add($"{connector} {inClause}");
            else
                _whereConditions.Add(inClause);

            return this;
        }

        // --- BETWEEN HELPERS ---
        public SqlQueryBuilder WhereBetween(string column, object start, object end)
        {
            return AddBetweenCondition(column, start, end, false, "AND");
        }

        public SqlQueryBuilder NotWhereBetween(string column, object start, object end)
        {
            return AddBetweenCondition(column, start, end, true, "AND");
        }

        public SqlQueryBuilder OrWhereBetween(string column, object start, object end)
        {
            return AddBetweenCondition(column, start, end, false, "OR");
        }

        public SqlQueryBuilder OrNotWhereBetween(string column, object start, object end)
        {
            return AddBetweenCondition(column, start, end, true, "OR");
        }

        // --- Private helper for BETWEEN ---
        private SqlQueryBuilder AddBetweenCondition(string column, object start, object end, bool notBetween, string connector)
        {
            if (start == null || end == null)
                throw new ArgumentException("Start and end values for BETWEEN cannot be null");

            string paramStart = $"@{column}Start{_parameters.Count}";
            string paramEnd = $"@{column}End{_parameters.Count}";

            _parameters.Add(new SqlParameter(paramStart, start));
            _parameters.Add(new SqlParameter(paramEnd, end));

            string condition = $"{column} {(notBetween ? "NOT BETWEEN" : "BETWEEN")} {paramStart} AND {paramEnd}";

            if (_whereConditions.Count > 0)
                _whereConditions.Add($"{connector} {condition}");
            else
                _whereConditions.Add(condition);

            return this;
        }


        public SqlQueryBuilder Join(string joinClause)
        {
            _joins.Add(joinClause);
            return this;
        }

        public SqlQueryBuilder InnerJoin(string table, string condition)
        {
            _joins.Add($"INNER JOIN {table} ON {condition}");
            return this;
        }

        public SqlQueryBuilder LeftJoin(string table, string condition)
        {
            _joins.Add($"LEFT JOIN {table} ON {condition}");
            return this;
        }

        public SqlQueryBuilder RightJoin(string table, string condition)
        {
            _joins.Add($"RIGHT JOIN {table} ON {condition}");
            return this;
        }

        public SqlQueryBuilder FullJoin(string table, string condition)
        {
            _joins.Add($"FULL JOIN {table} ON {condition}");
            return this;
        }


        public SqlQueryBuilder OrderBy(string orderBy)
        {
            _orderBy = orderBy;
            return this;
        }

        public SqlQueryBuilder Limit(int limit)
        {
            _limit = limit;
            return this;
        }

        // --- GROUP BY & HAVING ---
        public SqlQueryBuilder GroupBy(params string[] columns)
        {
            _groupByColumns.AddRange(columns);
            return this;
        }

        public SqlQueryBuilder Having(string condition, SqlParameter parameter)
        {
            _havingConditions.Add(condition);
            _parameters.Add(parameter);
            return this;
        }

        // --- INSERT ---
        public SqlQueryBuilder InsertInto(string table)
        {
            _queryType = QueryType.Insert;
            _table = table;
            return this;
        }

        public SqlQueryBuilder Value(string column, SqlParameter value)
        {
            if (_queryType == QueryType.Insert)
                _insertValues[column] = value;
            else if (_queryType == QueryType.Update)
                _updateValues[column] = value;

            return this;
        }

        // --- UPDATE ---
        public SqlQueryBuilder Update(string table)
        {
            _queryType = QueryType.Update;
            _table = table;
            return this;
        }

        // --- DELETE ---
        public SqlQueryBuilder DeleteFrom(string table)
        {
            _queryType = QueryType.Delete;
            _table = table;
            return this;
        }

        // --- RAW SQL ---
        public SqlQueryBuilder Raw(string rawSql, params SqlParameter[] parameters)
        {
            _rawSql = rawSql;
            _rawParameters.AddRange(parameters);
            return this;
        }

        // --- Build Query ---
        public string BuildQuery()
        {
            if (!string.IsNullOrWhiteSpace(_rawSql))
                return _rawSql;

            if (string.IsNullOrEmpty(_table))
                throw new InvalidOperationException("Table name not specified.");

            var sql = new StringBuilder();

            switch (_queryType)
            {
                case QueryType.Select:
                    sql.Append("SELECT ");

                    if (_isDistinct)
                        sql.Append("DISTINCT ");

                    if (_top.HasValue)
                        sql.Append("TOP ").Append(_top.Value).Append(" ");

                    sql.Append(_columns.Count > 0 ? string.Join(", ", _columns) : "*");
                    sql.Append(" FROM ").Append(_table);

                    if (_joins.Count > 0)
                        sql.Append(" ").Append(string.Join(" ", _joins));

                    if (_whereConditions.Count > 0)
                        sql.Append(" WHERE ").Append(string.Join(" ", _whereConditions));

                    if (_groupByColumns.Count > 0)
                        sql.Append(" GROUP BY ").Append(string.Join(", ", _groupByColumns));

                    if (_havingConditions.Count > 0)
                        sql.Append(" HAVING ").Append(string.Join(" AND ", _havingConditions));

                    if (!string.IsNullOrEmpty(_orderBy))
                        sql.Append(" ORDER BY ").Append(_orderBy);

                    if (_limit.HasValue)
                        sql.Append(" OFFSET 0 ROWS FETCH NEXT ").Append(_limit.Value).Append(" ROWS ONLY");
                    break;

                case QueryType.Insert:
                    var columns = string.Join(", ", _insertValues.Keys);
                    var paramNames = string.Join(", ", _insertValues.Values.Select(p => p.ParameterName));
                    sql.Append($"INSERT INTO {_table} ({columns}) VALUES ({paramNames})");
                    break;

                case QueryType.Update:
                    var setClauses = new List<string>();
                    foreach (var kvp in _updateValues)
                        setClauses.Add($"{kvp.Key} = {kvp.Value.ParameterName}");

                    sql.Append($"UPDATE {_table} SET ");
                    sql.Append(string.Join(", ", setClauses));

                    if (_whereConditions.Count > 0)
                        sql.Append(" WHERE ").Append(string.Join(" AND ", _whereConditions));
                    break;

                case QueryType.Delete:
                    sql.Append($"DELETE FROM {_table}");

                    if (_whereConditions.Count > 0)
                        sql.Append(" WHERE ").Append(string.Join(" AND ", _whereConditions));
                    break;
            }

            return sql.ToString();
        }

        // --- Get all parameters ---
        public SqlParameter[] GetParameters()
        {
            if (!string.IsNullOrWhiteSpace(_rawSql))
                return _rawParameters.ToArray();

            var allParams = new List<SqlParameter>(_parameters);

            if (_queryType == QueryType.Insert)
                allParams.AddRange(_insertValues.Values);
            else if (_queryType == QueryType.Update)
                allParams.AddRange(_updateValues.Values);

            return allParams.ToArray();
        }
    }
}

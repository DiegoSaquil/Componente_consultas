using System;
using System.Data;
using System.IO;
using System.Collections.Generic;
using Capa_Modelo_Componente_Consultas;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;

namespace Capa_Controlador_Consultas

// Realizado por: Nelson Jose Godínez Méndez 0901-22-3550 22/09/2025 
{
    public class Controlador : IDisposable
    {
        #region Campos (privados)
        private readonly string _dsn; //Nombre del origen de datos de odbc DSN
        private readonly string _db; // Nombre de la base de datos 
        //Encapsulación de sentencias y conexión
        private readonly Conexion _cx;
        private readonly Sentencias _repo;

        private readonly string _filePathXml;
        #endregion
        #region Propiedades públicas (estándar "p" + PascalCase)
        #endregion
       
        #region Propiedades internas/compatibilidad
        public DataTable Queries { get; private set; }
        #endregion
        #region Ctor / Inicialización
        public Controlador(string dsn, string databaseName)
        {
            // Validación de parámetros, si no envía un DSN o nombre de BD
            if (dsn == null) throw new ArgumentNullException(nameof(dsn));
            if (databaseName == null) throw new ArgumentNullException(nameof(databaseName));
            //Guarda valores en dsn
            _dsn = dsn;
            _db = databaseName;

            _cx = new Conexion(_dsn);
            _repo = new Sentencias(_cx, _db);
            //Define la ruta para guardar las consultas en el directorio application data (APPDATA)
            var root = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            var dir = Path.Combine(root, "ConsultasUMG");
            Directory.CreateDirectory(dir); //Aquí crea la carpeta si no lo existe
            _filePathXml = Path.Combine(dir, "queries.xml"); 
            //Construye la tabla en memoria que almacenará las consultas
            Queries = BuildTable();
            LoadQueries();

            //Cada vez que se actualice, borra o agrega una fila y se guardan los cambios en el XML
            Queries.RowChanged += (s, e) => SaveQueries();
            Queries.RowDeleted += (s, e) => SaveQueries();
            Queries.TableNewRow += (s, e) => SaveQueries();
        }
        #endregion
        // ----------------------------------------------------------------------------------------

        // Realizado por: Juan Carlos Sandoval Quej 0901-22-4170 22/09/2025
        #region Consultas de esquema / ayuda a UI
        public List<string> ObtenerNombresTablas() => _repo.ObtenerNombresTablas();


        public DataTable ConsultarTabla(string tabla, bool asc)
        {
            var cols = _repo.ObtenerColumnas(tabla);
            return _repo.ConsultarTablaOrdenada(tabla, asc, cols);
        }
        #endregion
        #region Validación / Sugerencias
        public bool ValidarConsulta(string sql, out string razon)
        {
            razon = string.Empty;
            if (string.IsNullOrWhiteSpace(sql)) { razon = "La consulta está vacía."; return false; }

            string s = LimpiarInicio(sql).Trim();

            // Quitar ; final y prohibir múltiples sentencias
            if (s.EndsWith(";")) s = s.Substring(0, s.Length - 1).Trim();
            if (s.IndexOf(';') >= 0) { razon = "No se permiten múltiples sentencias."; return false; }

            string first = PrimerToken(s);
            string u = first.ToUpperInvariant();
            if (u == "SELECT" || u == "SHOW" || u == "DESCRIBE" || u == "DESC" || u == "EXPLAIN" || u == "WITH")
                return true;

            razon = "Solo se permiten sentencias de lectura: SELECT, SHOW, DESCRIBE/DESC, EXPLAIN o WITH.";
            return false;
        }

        public string SugerirNombre(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql)) return "Consulta";
            var flat = sql.Replace("\r", " ").Replace("\n", " ").Trim();
            if (flat.Length > 40) flat = flat.Substring(0, 40) + "…";
            return "Consulta: " + flat;
        }

        // ---------------------------------------------------------------------------------

        #endregion
        #region Ejecución de consultas
       
        // Realizado por: Raul
        public bool TryEjecutarConsulta(string sql, out DataTable result, out string error)
        {
            result = null;
            error = null;

            if (!ValidarConsulta(sql, out var razon))
            {
                error = razon;
                return false;
            }

            try
            {
                var sqlSeguro = RewriteSelectAllForOdbc(sql, _db);
                result = _repo.EjecutarSelect(sqlSeguro.TrimEnd(new[] { ';', ' ' }));
                return true;
            }
            catch (System.Data.Odbc.OdbcException ex)
            {
                if (ex.Message.IndexOf("Invalid time(hours)", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    error =
                        "ODBC falló con una columna TIME (>24h).\n" +
                        "Usa TIME_FORMAT(col,'%H:%i:%s') o CAST(col AS CHAR(10)) en tu SELECT.";
                }
                else
                {
                    error = "Error al ejecutar la consulta:\n" + ex.Message;
                }
                return false;
            }
            catch (Exception ex)
            {
                error = "Error inesperado al ejecutar la consulta:\n" + ex.Message;
                return false;
            }
        }
        public bool TryPreviewById(string id, out DataTable result, out string error)
        {
            result = null; error = null;
            var data = GetQuery(id);
            if (data == null) { error = "Consulta no encontrada."; return false; }
            return TryEjecutarConsulta(data.Item2, out result, out error);
        }
        #endregion
        #region CRUD sobre Queries (persistidas en XML)
        public DataRow AddQuery(string name, string sql)
        {
            var row = Queries.NewRow();
            row["Id"] = Guid.NewGuid().ToString();
            row["Name"] = string.IsNullOrWhiteSpace(name) ? "Consulta" : name;
            row["Sql"] = sql ?? string.Empty;
            Queries.Rows.Add(row);
            return row;
        }

        public void UpdateQuery(string id, string name, string sql)
        {
            var row = FindById(id);
            if (row == null) return;
            row["Name"] = string.IsNullOrWhiteSpace(name) ? "Consulta" : name;
            row["Sql"] = sql ?? string.Empty;
        }

        public void DeleteQuery(string id)
        {
            var row = FindById(id);
            if (row != null) row.Delete();
        }

        public Tuple<string, string> GetQuery(string id)
        {
            var row = FindById(id);
            if (row == null) return null;
            return Tuple.Create(
                row["Name"] != null ? row["Name"].ToString() : "Consulta",
                row["Sql"] != null ? row["Sql"].ToString() : string.Empty
            );
        }

        private DataRow FindById(string id)
        {
            foreach (DataRow r in Queries.Rows)
                if (string.Equals(Convert.ToString(r["Id"]), id, StringComparison.OrdinalIgnoreCase))
                    return r;
            return null;
        }

        private static DataTable BuildTable()
        {
            var dt = new DataTable("Queries");
            dt.Columns.Add("Id", typeof(string));
            dt.Columns.Add("Name", typeof(string));
            dt.Columns.Add("Sql", typeof(string));
            return dt;
        }

        private void LoadQueries()
        {
            try
            {
                if (!File.Exists(_filePathXml)) return;

                var ds = new DataSet();
                ds.ReadXml(_filePathXml);

                Queries.Clear();

                DataTable t = null;
                foreach (DataTable dt in ds.Tables)
                {
                    if (string.Equals(dt.TableName, "Queries", StringComparison.OrdinalIgnoreCase))
                    { t = dt; break; }
                }
                if (t == null && ds.Tables.Count > 0) t = ds.Tables[0];
                if (t == null) return;

                if (!t.Columns.Contains("Id")) t.Columns.Add("Id", typeof(string));
                if (!t.Columns.Contains("Name")) t.Columns.Add("Name", typeof(string));
                if (!t.Columns.Contains("Sql")) t.Columns.Add("Sql", typeof(string));

                foreach (DataRow r in t.Rows)
                {
                    var n = Queries.NewRow();
                    n["Id"] = Convert.ToString(r["Id"] ?? Guid.NewGuid().ToString());
                    n["Name"] = Convert.ToString(r["Name"] ?? "Consulta");
                    n["Sql"] = Convert.ToString(r["Sql"] ?? string.Empty);
                    Queries.Rows.Add(n);
                }
            }
            catch
            {
                // si falla, empezamos vacío
            }
        }

        private void SaveQueries()
        {
            try
            {
                var ds = new DataSet("Store");
                ds.Tables.Add(Queries.Copy());
                ds.Tables[0].TableName = "Queries";
                ds.WriteXml(_filePathXml, XmlWriteMode.WriteSchema);
            }
            catch
            {
                // opcional: log
            }
        }
        // --------------------------------------------------------------------------
        #endregion

        // Realizado por: DIEGO

        //linea 289 a 295 IDisponible
        #region IDisposable
        public void Dispose()
        {
            _cx?.Dispose();
        }
        #endregion
        #region Helpers de parsing / reescritura
        private static string LimpiarInicio(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql)) return string.Empty;
            int i = 0;
            while (i < sql.Length)
            {
                while (i < sql.Length && char.IsWhiteSpace(sql[i])) i++;

                if (i + 1 < sql.Length && sql[i] == '-' && sql[i + 1] == '-')
                { while (i < sql.Length && sql[i] != '\n') i++; continue; }

                if (i + 1 < sql.Length && sql[i] == '/' && sql[i + 1] == '*')
                {
                    i += 2;
                    while (i + 1 < sql.Length && !(sql[i] == '*' && sql[i + 1] == '/')) i++;
                    if (i + 1 < sql.Length) i += 2;
                    continue;
                }
                break;
            }
            return sql.Substring(i);
        }

        private static string PrimerToken(string s)
        {
            int i = 0;
            while (i < s.Length && char.IsWhiteSpace(s[i])) i++;
            int start = i;
            while (i < s.Length && !char.IsWhiteSpace(s[i]) && s[i] != '(' && s[i] != '/')
                i++;
            return s.Substring(start, i - start);
        }

        private string BuildSelectAllQuery(string tabla, string schema)
        {
            var cols = GetColumns(schema, tabla);
            if (cols == null || cols.Count == 0)
                return $"SELECT * FROM `{tabla}`";

            var lista = cols.Select(c =>
            {
                string cb = $"`{c.ColumnName}`";
                if (string.Equals(c.DataType, "time", StringComparison.OrdinalIgnoreCase))
                    return $"CAST({cb} AS CHAR(10)) AS {cb}";
                return cb;
            });

            var sb = new StringBuilder();
            sb.Append("SELECT ").Append(string.Join(", ", lista))
              .Append(" FROM `").Append(tabla).Append('`');
            return sb.ToString();
        }

        public string RewriteSelectAllForOdbc(string sql, string schema)
        {
            if (string.IsNullOrWhiteSpace(sql)) return sql;

            var s = sql.Trim().TrimEnd(';');
            var rx = new Regex(@"^\s*select\s+\*\s+from\s+`?([A-Za-z0-9_]+)`?\s*$",
                               RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
            var m = rx.Match(s);
            if (!m.Success) return sql;  

            var tabla = m.Groups[1].Value;
            return BuildSelectAllQuery(tabla, schema) + ";";
        }

        public DataTable ConsultarTablaSeguro(string tabla, bool asc)
        {
            string q = BuildSelectAllQuery(tabla, _db);
            q += asc ? " ORDER BY 1 ASC" : " ORDER BY 1 DESC";
            return _repo.EjecutarSelect(q);
        }
        #endregion
        #region Lectura de columnas desde information_schema
        private sealed class ColInfo
        {
            public string ColumnName { get; set; }
            public string DataType { get; set; }
        }

        private static string Esc(string s)
        {
            if (s == null) return string.Empty;
            return s.Replace("`", "").Replace("'", "''");
        }

        private List<ColInfo> GetColumns(string schema, string tabla)
        {
            var list = new List<ColInfo>();
            string sql =
                "SELECT COLUMN_NAME, DATA_TYPE " +
                "FROM information_schema.COLUMNS " +
                $"WHERE TABLE_SCHEMA = '{Esc(schema)}' AND TABLE_NAME = '{Esc(tabla)}' " +
                "ORDER BY ORDINAL_POSITION";

            var dt = _repo.EjecutarSelect(sql);
            foreach (DataRow r in dt.Rows)
            {
                list.Add(new ColInfo
                {
                    ColumnName = Convert.ToString(r["COLUMN_NAME"]),
                    DataType = Convert.ToString(r["DATA_TYPE"])
                });
            }
            return list;
        }
        #endregion
        // ------------------------------------------------------------------------------------------------
    }
}

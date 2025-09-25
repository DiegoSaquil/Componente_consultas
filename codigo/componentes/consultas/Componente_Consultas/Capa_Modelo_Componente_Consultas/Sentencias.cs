using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Text;

namespace Capa_Modelo_Componente_Consultas
{
    // Diego Fernando Saquil Gramajo 0901 - 22 - 4103 Guatemala 23 de septiembre
    public class ColumnaInfo
    {
        public string Nombre { get; set; }
        public string Tipo { get; set; } 
    }

#region Acceso a datos
    // Acceso a datos 
    public class Sentencias
    {
        private readonly string _db;
        private readonly Conexion _cx;

        public Sentencias(Conexion cx, string databaseName)
        {
            _cx = cx ?? throw new ArgumentNullException(nameof(cx));
            _db = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
        }

        public List<string> ObtenerNombresTablas()
        {
            var tablas = new List<string>();
            const string sql = @"
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = ?
                ORDER BY TABLE_NAME;";

            var cn = _cx.Abrir();
            using (var cmd = new OdbcCommand(sql, cn))
            {
                cmd.Parameters.AddWithValue("@p1", _db);
                using (var rd = cmd.ExecuteReader())
                {
                    while (rd.Read()) tablas.Add(rd.GetString(0));
                }
            }
            return tablas;
        }

        public List<ColumnaInfo> ObtenerColumnas(string tabla)
        {
            var cols = new List<ColumnaInfo>();
            const string sql = @"
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
                ORDER BY ORDINAL_POSITION;";

            var cn = _cx.Abrir();
            using (var cmd = new OdbcCommand(sql, cn))
            {
                cmd.Parameters.AddWithValue("@p1", _db);
                cmd.Parameters.AddWithValue("@p2", tabla);
                using (var rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        cols.Add(new ColumnaInfo
                        {
                            Nombre = rd.GetString(0),
                            Tipo = rd.GetString(1)
                        });
                    }
                }
            }
            return cols;
        }
        // ----------------------------------------------------------------------------------------- //
        // ----------------------------------------------------------------------------------------- //

        // Realizado por: Bryan Raul Ramirez Lopez 0901-21-8202 22/09/2025

        // Método para consultar una tabla y devolverla ordenada
        public DataTable ConsultarTablaOrdenada(string tabla, bool asc, List<ColumnaInfo> columnas)
        {
            // Si no se envían columnas o está vacía la lista, se devuelve un DataTable vacío
            if (columnas == null || columnas.Count == 0) return new DataTable();

            // Lista para almacenar las columnas que se van a seleccionar en el SELECT
            var selectCols = new List<string>(columnas.Count);
            // Recorremos todas las columnas enviadas en la lista
            foreach (var c in columnas)
            {
            // Si el tipo de la columna es "time" (hora), se castea a CHAR(10) para evitar problemas al mostrarla
                if (string.Equals(c.Tipo, "time", StringComparison.OrdinalIgnoreCase))
                    selectCols.Add("CAST(`" + c.Nombre + "` AS CHAR(10)) AS `" + c.Nombre + "`");
                else
           // Caso contrario, se agrega directamente el nombre de la columna
                    selectCols.Add("`" + c.Nombre + "`");
            }
            // Se define el orden de la consulta: ASC si es ascendente, DESC si es descendente
            string order = asc ? "ASC" : "DESC";
            // Se toma la primera columna de la lista para usarla en la cláusula ORDER BY
            string firstCol = columnas[0].Nombre;
            // Se construye la consulta SQL completa
            string sql = "SELECT " + string.Join(", ", selectCols) +
                         " FROM `" + _db + "`.`" + tabla + "` ORDER BY `" + firstCol + "` " + order + ";";
            // Abre la conexión con la base de datos
            var cn = _cx.Abrir();
            // Se usa un DataAdapter para ejecutar la consulta y llenar el DataTable
            using (var da = new OdbcDataAdapter())
            {
                // Se asigna el comando SQL al DataAdapter
                da.SelectCommand = new OdbcCommand(sql, cn); 
                var dt = new DataTable();
                // Llena el DataTable con los resultados de la consulta
                da.Fill(dt);

                // Devuelve el DataTable ya con los datos obtenidos
                return dt;
            }
        }

        // Método para ejecutar un SELECT cualquiera (ya armado como string) y devolver el resultado
        public DataTable EjecutarSelect(string sql)
        {
            // Abre la conexión con la base de datos
            var cn = _cx.Abrir();

            // Se usa un DataAdapter para ejecutar la consulta y llenar el DataTable
            using (var da = new OdbcDataAdapter())
            {
                // Se asigna el comando SQL al DataAdapter
                da.SelectCommand = new OdbcCommand(sql, cn);

                // Se crea un DataTable vacío para almacenar los resultados
                var dt = new DataTable();

                // Llena el DataTable con los resultados de la consulta SQL
                da.Fill(dt);

                // Devuelve el DataTable ya con los datos
                return dt;
            }
        }
    
#endregion
}
}

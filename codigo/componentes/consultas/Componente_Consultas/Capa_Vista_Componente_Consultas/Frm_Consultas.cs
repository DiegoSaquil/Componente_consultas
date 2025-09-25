using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
namespace Capa_Vista_Componente_Consultas
{
    public partial class Frm_Consultas : Form
    {
        private const string DSN = "Prueba1";
        private const string DB = "controlempleados";

        private string _ultimaTabla = null;
        private List<string> _ultimasColumnas = null;
        private string _ultimaColumnaOrden = null;

        public Frm_Consultas()
        {
            InitializeComponent();
            this.FormBorderStyle = FormBorderStyle.None;
            this.ControlBox = false;

            this.Load += Frm_Consultas_Load;
            Btn_buscar.Click += Btn_buscar_Click;
            Rdb_ASC.CheckedChanged += Orden_CheckedChanged;
            Rdb_Opcion1.CheckedChanged += Orden_CheckedChanged;
        }

        private OdbcConnection GetConn()
        {
            return new OdbcConnection($"Dsn={DSN};Database={DB}");
        }

        
        private void Frm_Consultas_Load(object sender, EventArgs e)
        {
            try
            {
                using (var cn = GetConn())
                using (var cmd = cn.CreateCommand())
                {
                    cn.Open();
                    cmd.CommandText = @"SELECT table_name
                                        FROM information_schema.tables
                                        WHERE table_schema = ?
                                        ORDER BY table_name";
                    cmd.Parameters.Add("schema", OdbcType.VarChar).Value = DB;

                    var dt = new DataTable();
                    using (var da = new OdbcDataAdapter(cmd)) { da.Fill(dt); }

                    Cbo_Query.DropDownStyle = ComboBoxStyle.DropDownList;
                    Cbo_Query.DisplayMember = "table_name";
                    Cbo_Query.ValueMember = "table_name";
                    Cbo_Query.DataSource = dt;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show("No se pudieron cargar las tablas.\n" + ex.Message, "ODBC",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
            }

           
            Txt_Consulta.ReadOnly = false;
            Txt_Consulta.Enabled = true;
            Txt_Consulta.TabStop = true;
            Txt_Consulta.BackColor = SystemColors.Window;
            Txt_Consulta.ForeColor = SystemColors.WindowText;
        }

        
        private static List<(string Col, string DataType)> GetColsTipos(OdbcConnection cn, string tabla)
        {
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = @"SELECT column_name, data_type
                                    FROM information_schema.columns
                                    WHERE table_schema = ? AND table_name = ?
                                    ORDER BY ordinal_position";
                cmd.Parameters.Add("schema", OdbcType.VarChar).Value = DB;
                cmd.Parameters.Add("table", OdbcType.VarChar).Value = tabla;

                var list = new List<(string, string)>();
                using (var rd = cmd.ExecuteReader())
                {
                    while (rd.Read()) list.Add((rd.GetString(0), rd.GetString(1)));
                }
                return list;
            }
        }

        private static string GetPrimeraColumna(OdbcConnection cn, string tabla)
        {
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = @"SELECT column_name
                                    FROM information_schema.columns
                                    WHERE table_schema = ? AND table_name = ?
                                    ORDER BY ordinal_position LIMIT 1";
                cmd.Parameters.Add("schema", OdbcType.VarChar).Value = DB;
                cmd.Parameters.Add("table", OdbcType.VarChar).Value = tabla;
                object r = cmd.ExecuteScalar();
                return r == null ? null : r.ToString();
            }
        }

      
        private static string FindEmpleadoTable(OdbcConnection cn)
        {
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = @"
                    SELECT c1.table_name
                    FROM information_schema.columns c1
                    JOIN information_schema.columns c2
                      ON c1.table_schema = c2.table_schema
                     AND c1.table_name  = c2.table_name
                    WHERE c1.table_schema = ?
                      AND c1.column_name = 'codigo_empleado'
                      AND c2.column_name = 'nombre_completo'
                    LIMIT 1";
                cmd.Parameters.Add("schema", OdbcType.VarChar).Value = DB;
                object r = cmd.ExecuteScalar();
                return r == null ? null : r.ToString();
            }
        }

      
        private static string BuildSelectListCasting(IEnumerable<(string Col, string DataType)> colsTipos,
                                                     List<string> soloEstasOCeroNull)
        {
            var tiposCast = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            { "time", "date", "datetime", "timestamp" };

            IEnumerable<(string Col, string DataType)> cols = colsTipos;

            if (soloEstasOCeroNull != null)
            {
                var set = new HashSet<string>(soloEstasOCeroNull, StringComparer.OrdinalIgnoreCase);
                cols = colsTipos.Where(ct => set.Contains(ct.Col));
            }

            var piezas = cols.Select(ct =>
                tiposCast.Contains(ct.DataType)
                    ? $"CAST(`{ct.Col}` AS CHAR) AS `{ct.Col}`"
                    : $"`{ct.Col}`"
            );

            return string.Join(", ", piezas);
        }

  
        private void Btn_buscar_Click(object sender, EventArgs e)
        {
            if (Cbo_Query.SelectedValue == null)
            {
                MessageBox.Show("Selecciona una tabla.", "Consulta", MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            string tabla = Cbo_Query.SelectedValue.ToString();
            string entrada = (Txt_Consulta.Text ?? "").Trim();

           
            if (!string.IsNullOrWhiteSpace(entrada))
            {
                bool hallado = BuscarPorNombreMostrarTodo(entrada);
                if (hallado)
                {
                    _ultimaTabla = null; _ultimasColumnas = null; _ultimaColumnaOrden = null;
                    return; 
                }
            }

           
            try
            {
                using (var cn = GetConn())
                {
                    cn.Open();

                    var colsTipos = GetColsTipos(cn, tabla);
                    var columnasValidas = new HashSet<string>(
                        colsTipos.Select(x => x.Col), StringComparer.OrdinalIgnoreCase);

                    List<string> pedidas = null;
                    if (!string.IsNullOrWhiteSpace(entrada))
                    {
                        pedidas = entrada.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                                         .Select(c => c.Trim())
                                         .Where(c => c.Length > 0)
                                         .ToList();

                        var noExisten = pedidas.Where(p => !columnasValidas.Contains(p)).ToList();
                        if (noExisten.Any())
                        {
                            MessageBox.Show("Estas columnas no existen en '" + tabla + "':\n- " +
                                string.Join("\n- ", noExisten),
                                "Columnas inválidas", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                            return;
                        }
                    }

                    string selectList = BuildSelectListCasting(colsTipos, pedidas);

                    string colOrden = (pedidas != null && pedidas.Count > 0)
                                        ? pedidas[0]
                                        : PreferColOrden(colsTipos) ?? GetPrimeraColumna(cn, tabla);
                    _ultimaColumnaOrden = colOrden;

                    string order = "";
                    if (Rdb_ASC.Checked || (Rdb_Opcion1 != null && Rdb_Opcion1.Checked))
                    {
                        string dir = Rdb_ASC.Checked ? "Rdb_ASC" : "DESC";
                        if (!string.IsNullOrEmpty(colOrden))
                            order = $" ORDER BY `{colOrden}` {dir}";
                    }

                    string sql = $"SELECT {selectList} FROM `{tabla}`{order} LIMIT 500;";

                    var dt = new DataTable();
                    using (var da = new OdbcDataAdapter(sql, cn)) { da.Fill(dt); }
                    Dgv_Consultas_Simples.DataSource = dt;

                    _ultimaTabla = tabla;
                    _ultimasColumnas = pedidas;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error al ejecutar la consulta.\n" + ex.Message,
                    "Consulta", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        
        private static string PreferColOrden(List<(string Col, string DataType)> colsTipos)
        {
            var pref = colsTipos.FirstOrDefault(ct => ct.Col.Equals("nombre_completo", StringComparison.OrdinalIgnoreCase));
            return string.IsNullOrEmpty(pref.Col) ? null : pref.Col;
        }

        
        private void Orden_CheckedChanged(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(_ultimaTabla)) return;

            try
            {
                using (var cn = GetConn())
                {
                    cn.Open();

                    var colsTipos = GetColsTipos(cn, _ultimaTabla);
                    string selectList = BuildSelectListCasting(colsTipos, _ultimasColumnas);

                    string colOrden = _ultimaColumnaOrden;
                    if (string.IsNullOrEmpty(colOrden))
                        colOrden = PreferColOrden(colsTipos) ?? GetPrimeraColumna(cn, _ultimaTabla);

                    string order = "";
                    if (Rdb_ASC.Checked || (Rdb_Opcion1 != null && Rdb_Opcion1.Checked))
                    {
                        string dir = Rdb_ASC.Checked ? "Rdb_ASC" : "DESC";
                        if (!string.IsNullOrEmpty(colOrden))
                            order = $" ORDER BY `{colOrden}` {dir}";
                    }

                    string sql = $"SELECT {selectList} FROM `{_ultimaTabla}`{order} LIMIT 500;";
                    var dt = new DataTable();
                    using (var da = new OdbcDataAdapter(sql, cn)) { da.Fill(dt); }
                    Dgv_Consultas_Simples.DataSource = dt;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show("No se pudo reordenar.\n" + ex.Message, "Orden",
                    MessageBoxButtons.OK, MessageBoxIcon.Warning);
            }
        }

        private bool BuscarPorNombreMostrarTodo(string nombre)
        {
            try
            {
                using (var cn = GetConn())
                {
                    cn.Open();

                   
                    string empTable = FindEmpleadoTable(cn);
                    if (string.IsNullOrEmpty(empTable))
                        return false;

                  
                    var codigos = new List<int>();
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandText = $"SELECT codigo_empleado FROM `{empTable}` " +
                                          "WHERE nombre_completo LIKE ? " +
                                          "ORDER BY nombre_completo LIMIT 50";
                        cmd.Parameters.Add("p", OdbcType.VarChar).Value = "%" + nombre + "%";
                        using (var rd = cmd.ExecuteReader())
                            while (rd.Read()) codigos.Add(rd.GetInt32(0));
                    }

                    if (codigos.Count == 0)
                        return false;

                
                    var tablas = new List<string>();
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandText = @"SELECT table_name
                                            FROM information_schema.columns
                                            WHERE table_schema = ? AND column_name = 'codigo_empleado'
                                            ORDER BY table_name";
                        cmd.Parameters.Add("schema", OdbcType.VarChar).Value = DB;
                        using (var rd = cmd.ExecuteReader())
                            while (rd.Read()) tablas.Add(rd.GetString(0));
                    }
                    if (tablas.Count == 0) return false;

                  
                    var allColsSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    var colsPorTabla = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

                    foreach (var t in tablas)
                    {
                        var colsTipos = GetColsTipos(cn, t);
                        var setCols = new HashSet<string>(colsTipos.Select(ct => ct.Col), StringComparer.OrdinalIgnoreCase);
                        colsPorTabla[t] = setCols;
                        foreach (var c in setCols) allColsSet.Add(c);
                    }

              
                    var orderedCols = new List<string>();
                    if (allColsSet.Contains("codigo_empleado")) { orderedCols.Add("codigo_empleado"); allColsSet.Remove("codigo_empleado"); }
                    if (allColsSet.Contains("nombre_completo")) { orderedCols.Add("nombre_completo"); allColsSet.Remove("nombre_completo"); }
                    orderedCols.AddRange(allColsSet.OrderBy(s => s, StringComparer.OrdinalIgnoreCase));

                    
                    var unionSql = new StringBuilder();
                    var parametros = new List<OdbcParameter>();

                    for (int i = 0; i < tablas.Count; i++)
                    {
                        if (i > 0) unionSql.AppendLine("UNION ALL");

                        var colsTabla = colsPorTabla[tablas[i]];

                        
                        var parts = new List<string>();
                        foreach (var col in orderedCols)
                        {
                            if (colsTabla.Contains(col))
                                parts.Add($"CAST(`{col}` AS CHAR) AS `{col}`");
                            else
                                parts.Add($"CAST(NULL AS CHAR) AS `{col}`");
                        }
                        string selectList = string.Join(", ", parts);

                        unionSql.AppendLine(
                            $"SELECT '{tablas[i]}' AS _tabla, {selectList} " +
                            $"FROM `{tablas[i]}` WHERE `codigo_empleado` IN (");

                        for (int j = 0; j < codigos.Count; j++)
                        {
                            if (j > 0) unionSql.Append(", ");
                            unionSql.Append("?");
                            parametros.Add(new OdbcParameter("p", OdbcType.Int) { Value = codigos[j] });
                        }
                        unionSql.AppendLine(")");
                    }

                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandText = unionSql.ToString();
                        cmd.Parameters.AddRange(parametros.ToArray());

                        var dt = new DataTable();
                        using (var da = new OdbcDataAdapter(cmd)) { da.Fill(dt); }
                        Dgv_Consultas_Simples.DataSource = dt;
                    }

                  
                    _ultimaTabla = null;
                    _ultimasColumnas = null;
                    _ultimaColumnaOrden = null;

                    return true;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error al buscar por nombre.\n" + ex.Message, "Búsqueda",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
                return true; 
            }
        }

        
        private void button1_Click(object sender, EventArgs e)
        {
            if (this.Owner != null) this.Owner.Show();
            this.Close();
        }
        private void btn_max_Click(object sender, EventArgs e)
        {
            this.WindowState = (this.WindowState == FormWindowState.Normal)
                ? FormWindowState.Maximized
                : FormWindowState.Normal;
        }
        private void btn_min_Click(object sender, EventArgs e)
        {
            this.WindowState = FormWindowState.Minimized;
        }
    }
}

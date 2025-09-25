using System;
using System.Data;
using System.Windows.Forms;
using Capa_Controlador_Consultas;

namespace Capa_Vista_Componente_Consultas
{
    public partial class Frm_Consulta_Compleja : Form

        // Realizado por: Diego Fernando Saquil Gramajo 0901-22-4103
    {
        private const string DSN = "Prueba1";
        private const string DB = "controlempleados";

        private Controlador _ctrl;
        private bool _uiBusy = false;

        public Frm_Consulta_Compleja()
        {
            InitializeComponent();

            this.Load -= Consulta_Compleja_Load;
            this.Load += Consulta_Compleja_Load;

            if (Rdb_asc != null) Rdb_asc.Checked = true;
            if (Rdb_asc != null) { Rdb_asc.CheckedChanged -= Rb_Orden_CheckedChanged; Rdb_asc.CheckedChanged += Rb_Orden_CheckedChanged; }
            if (Rdb_desc != null) { Rdb_desc.CheckedChanged -= Rb_Orden_CheckedChanged; Rdb_desc.CheckedChanged += Rb_Orden_CheckedChanged; }

            if (Btn_buscar != null) { Btn_buscar.Click -= Btn_buscar_Click; Btn_buscar.Click += Btn_buscar_Click; }
            if (Btn_EjecutarSQL != null) { Btn_EjecutarSQL.Click -= Btn_EjecutarSQL_Click; Btn_EjecutarSQL.Click += Btn_EjecutarSQL_Click; }
            if (Btn_editar != null) { Btn_editar.Click -= Btn_editar_Click; Btn_editar.Click += Btn_editar_Click; }
            if (Btn_eliminar != null) { Btn_eliminar.Click -= Btn_eliminar_Click; Btn_eliminar.Click += Btn_eliminar_Click; }

            if (Btn_consimple != null) { Btn_consimple.Click -= Btn_consimple_Click; Btn_consimple.Click += Btn_consimple_Click; }
            if (Btn_regresar != null) { Btn_regresar.Click -= Btn_regresar_Click; Btn_regresar.Click += Btn_regresar_Click; }
            if (Btn_min != null) { Btn_min.Click -= Btn_min_Click; Btn_min.Click += Btn_min_Click; }
            if (Btn_max != null) { Btn_max.Click -= Btn_max_Click; Btn_max.Click += Btn_max_Click; }

            if (Lst_Querys != null)
            {
                Lst_Querys.SelectedIndexChanged -= Lst_Querys_SelectedIndexChanged;
                Lst_Querys.SelectedIndexChanged += Lst_Querys_SelectedIndexChanged;
            }
        }
        private void Consulta_Compleja_Load(object sender, EventArgs e)
        {
            try
            {
                _ctrl = new Controlador(DSN, DB);

                Cbo_busqueda.DataSource = _ctrl.ObtenerNombresTablas();

                Lst_Querys.DataSource = _ctrl.Queries;
                Lst_Querys.DisplayMember = "Name";
                Lst_Querys.ValueMember = "Id";
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error inicializando: " + ex.Message,
                    "Consultas", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private void Btn_buscar_Click(object sender, EventArgs e) => RefrescarTabla();

        private void Rb_Orden_CheckedChanged(object sender, EventArgs e)
        {
            if (Cbo_busqueda != null && Cbo_busqueda.SelectedItem is string s && s.Length > 0)
                RefrescarTabla();
        }
        // ----------------------------------------------------------------------------------------- //

        
        // Realizado por:
        private void RefrescarTabla()
        {
            var tabla = Cbo_busqueda != null ? Cbo_busqueda.SelectedItem as string : null;
            if (string.IsNullOrWhiteSpace(tabla))
            {
                MessageBox.Show("Selecciona una tabla.");
                return;
            }
            try
            {
                bool asc = Rdb_asc != null ? Rdb_asc.Checked : true;
                var dt = _ctrl.ConsultarTabla(tabla, asc);

                Dgv_Tabla.DataSource = dt;
                Dgv_Tabla.ReadOnly = true;
                Dgv_Tabla.AllowUserToAddRows = false;
                Dgv_Tabla.AllowUserToDeleteRows = false;
                Dgv_Tabla.AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.AllCells;
            }
            catch (Exception ex)
            {
                MessageBox.Show("No se pudo consultar la tabla.\n" + ex.Message,
                    "ODBC / MySQL", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }
        private string PromptNombreConsulta(string sugerido)
        {
            using (var dlg = new Form())
            {
                dlg.Text = "Guardar consulta";
                dlg.StartPosition = FormStartPosition.CenterParent;
                dlg.Width = 520; dlg.Height = 160;
                dlg.FormBorderStyle = FormBorderStyle.FixedDialog;
                dlg.MaximizeBox = false; dlg.MinimizeBox = false;

                var lbl = new Label { Left = 12, Top = 15, AutoSize = true, Text = "Nombre de la consulta:" };
                var txt = new TextBox { Left = 12, Top = 38, Width = 480, Text = string.IsNullOrWhiteSpace(sugerido) ? "Consulta" : sugerido };
                var btnOk = new Button { Left = 316, Top = 72, Width = 85, DialogResult = DialogResult.OK, Text = "Guardar" };
                var btnCancel = new Button { Left = 407, Top = 72, Width = 85, DialogResult = DialogResult.Cancel, Text = "Cancelar" };

                dlg.Controls.Add(lbl);
                dlg.Controls.Add(txt);
                dlg.Controls.Add(btnOk);
                dlg.Controls.Add(btnCancel);
                dlg.AcceptButton = btnOk;
                dlg.CancelButton = btnCancel;

                return dlg.ShowDialog(this) == DialogResult.OK ? txt.Text.Trim() : null;
            }
        }

        private void Btn_EjecutarSQL_Click(object sender, EventArgs e)
        {
            if (_uiBusy) return;
            _uiBusy = true;
            try
            {
                DataTable result;
                string error;
                if (!_ctrl.TryEjecutarConsulta(Txt_Sql.Text, out result, out error))
                {
                    MessageBox.Show(error, "Consulta SQL", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    return;
                }

                Dgv_Sql.DataSource = result;
                Dgv_Sql.ReadOnly = true;
                Dgv_Sql.AllowUserToAddRows = false;
                Dgv_Sql.AllowUserToDeleteRows = false;
                Dgv_Sql.AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.AllCells;

                var nombre = PromptNombreConsulta(_ctrl.SugerirNombre(Txt_Sql.Text));
                if (!string.IsNullOrWhiteSpace(nombre))
                    _ctrl.AddQuery(nombre, Txt_Sql.Text);
            }
            finally { _uiBusy = false; }
        }

        // ----------------------------------------------------------------------------------------- //

        // Realizado por:
        private void Lst_Querys_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (_uiBusy) return;
            var id = Lst_Querys.SelectedValue as string;
            if (string.IsNullOrEmpty(id)) return;

            var data = _ctrl.GetQuery(id);
            if (data == null) return;

            Txt_Sql.Text = data.Item2;

            DataTable result; string error;
            if (_ctrl.TryPreviewById(id, out result, out error))
            {
                Dgv_Sql.DataSource = result;
                Dgv_Sql.ReadOnly = true;
                Dgv_Sql.AllowUserToAddRows = false;
                Dgv_Sql.AllowUserToDeleteRows = false;
                Dgv_Sql.AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.AllCells;
            }
        }

        private void Btn_editar_Click(object sender, EventArgs e)
        {
            if (_uiBusy) return;
            _uiBusy = true;
            try
            {
                var id = Lst_Querys.SelectedValue as string;
                if (string.IsNullOrEmpty(id)) { MessageBox.Show("Selecciona una consulta de la lista."); return; }

                var data = _ctrl.GetQuery(id);
                if (data == null) return;

                using (var dlg = new Form())
                {
                    dlg.Text = "Editar consulta";
                    dlg.StartPosition = FormStartPosition.CenterParent;
                    dlg.Width = 860; dlg.Height = 720;

                    var lbl1 = new Label { Text = "Nombre:", Left = 12, Top = 12, AutoSize = true };
                    var txtNombre = new TextBox { Left = 80, Top = 10, Width = 740, Text = data.Item1 };

                    var lbl2 = new Label { Text = "SQL (editable):", Left = 12, Top = 45, AutoSize = true };
                    var txtSql = new TextBox { Left = 12, Top = 65, Width = 808, Height = 260, Multiline = true, ScrollBars = ScrollBars.Both, Text = data.Item2, AcceptsTab = true };

                    var btnProbar = new Button { Text = "Probar", Left = 12, Top = 332, Width = 80 };
                    var lblRes = new Label { Text = "Resultado:", Left = 100, Top = 336, AutoSize = true };
                    var dgvPrev = new DataGridView { Left = 12, Top = 360, Width = 808, Height = 260, ReadOnly = true, AllowUserToAddRows = false, AllowUserToDeleteRows = false, AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.AllCells };

                    var btnOk = new Button { Text = "Guardar", Left = 640, Top = 630, Width = 85, DialogResult = DialogResult.OK };
                    var btnCancel = new Button { Text = "Cancelar", Left = 735, Top = 630, Width = 85, DialogResult = DialogResult.Cancel };

                    btnProbar.Click += (s, ev) =>
                    {
                        DataTable r; string err;
                        if (!_ctrl.TryEjecutarConsulta(txtSql.Text, out r, out err))
                        {
                            MessageBox.Show(err, "Consulta SQL", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                            return;
                        }
                        dgvPrev.DataSource = r;
                    };

                    dlg.Controls.AddRange(new Control[] { lbl1, txtNombre, lbl2, txtSql, btnProbar, lblRes, dgvPrev, btnOk, btnCancel });
                    dlg.AcceptButton = btnOk; dlg.CancelButton = btnCancel;

                    if (dlg.ShowDialog(this) == DialogResult.OK)
                    {
                        _ctrl.UpdateQuery(id, txtNombre.Text.Trim(), txtSql.Text);

                        DataTable r; string err;
                        if (_ctrl.TryEjecutarConsulta(txtSql.Text, out r, out err))
                            Dgv_Sql.DataSource = r;
                    }
                }
            }
            finally { _uiBusy = false; }
        }

        // ----------------------------------------------------------------------------------------- //

        // Realizado por:
        private void Btn_eliminar_Click(object sender, EventArgs e)
        {
            if (_uiBusy) return;
            _uiBusy = true;
            try
            {
                var id = Lst_Querys.SelectedValue as string;
                if (string.IsNullOrEmpty(id)) { MessageBox.Show("Selecciona una consulta de la lista."); return; }

                var data = _ctrl.GetQuery(id);
                if (data == null) return;

                using (var dlg = new Form())
                {
                    dlg.Text = "Confirmación";
                    dlg.StartPosition = FormStartPosition.CenterParent;
                    dlg.Width = 460; dlg.Height = 180;
                    dlg.FormBorderStyle = FormBorderStyle.FixedDialog;
                    dlg.MaximizeBox = false; dlg.MinimizeBox = false;

                    var lbl = new Label { Left = 12, Top = 12, Width = 420, Height = 60, Text = "¿Seguro que deseas eliminar esta consulta?\n\n" + data.Item1 };
                    var btnEliminar = new Button { Text = "Eliminar consulta", Left = 180, Top = 90, Width = 130, DialogResult = DialogResult.OK };
                    var btnCancel = new Button { Text = "Cancelar", Left = 320, Top = 90, Width = 100, DialogResult = DialogResult.Cancel };

                    dlg.Controls.Add(lbl);
                    dlg.Controls.Add(btnEliminar);
                    dlg.Controls.Add(btnCancel);
                    dlg.AcceptButton = btnEliminar; dlg.CancelButton = btnCancel;

                    if (dlg.ShowDialog(this) == DialogResult.OK)
                    {
                        _ctrl.DeleteQuery(id);
                        if (Txt_Sql.Text == data.Item2) { Txt_Sql.Clear(); Dgv_Sql.DataSource = null; }
                    }
                }
            }
            finally { _uiBusy = false; }
        }

        private void Btn_consimple_Click(object sender, EventArgs e)
        {
            using (var f = new Frm_Consultas())
            {
                this.Hide();
                f.ShowDialog(this);
                this.Show();
            }
        }

        private void Btn_regresar_Click(object sender, EventArgs e)
        {
            if (this.Owner != null) this.Owner.Show();
            this.Close();
        }

        private void Btn_min_Click(object sender, EventArgs e) => this.WindowState = FormWindowState.Minimized;

        private void Btn_max_Click(object sender, EventArgs e)
        {
            this.WindowState = (this.WindowState == FormWindowState.Normal) ? FormWindowState.Maximized : FormWindowState.Normal;
        }

        protected override void OnFormClosed(FormClosedEventArgs e)
        {
            base.OnFormClosed(e);
        }
    }
}
// ----------------------------------------------------------------------------------------- //

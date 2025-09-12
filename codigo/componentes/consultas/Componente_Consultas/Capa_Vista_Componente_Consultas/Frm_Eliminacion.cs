using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Capa_Vista_Componente_Consultas
{
    public partial class Frm_Eliminar : Form
    {
        public Frm_Eliminar()
        {
            InitializeComponent();
            this.FormBorderStyle = FormBorderStyle.None;
            this.ControlBox = false;
        }

        private void Frm_Eliminar_Load(object sender, EventArgs e)
        {

        }

        private void btn_cerrar_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        private void btn_min_Click(object sender, EventArgs e)
        {
            this.WindowState = FormWindowState.Minimized;
        }

        private void btn_max_Click(object sender, EventArgs e)
        {
            if (this.WindowState == FormWindowState.Normal)
                this.WindowState = FormWindowState.Maximized;
            else
                this.WindowState = FormWindowState.Normal;
        }

        private void consultasToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Frm_Consultas consultas = new Frm_Consultas();
            consultas.Show();
            this.Hide();
        }

        private void creacionToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Frm_Creacion creacion = new Frm_Creacion();
            creacion.Show();
            this.Hide();
        }

        private void btn_buscar_Click(object sender, EventArgs e)
        {
            // Sin funcionalidad aún
        }

        private void btn_actualizar_Click(object sender, EventArgs e)
        {
            // Sin funcionalidad aún
        }

        private void btn_eliminar_Click(object sender, EventArgs e)
        {
            // Sin funcionalidad aún
        }

        private void btn_consultas_Click(object sender, EventArgs e)
        {
            // Sin funcionalidad aún
        }
    }
}

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
    public partial class Frm_Creacion : Form
    {
        public Frm_Creacion()
        {
            InitializeComponent();
            this.FormBorderStyle = FormBorderStyle.None;
            this.ControlBox = false;
        }

        private void editarToolStripMenuItem_Click(object sender, EventArgs e)
        {
            //esto es para moverse al formulario editar y eliminar
        }

        private void consultasToolStripMenuItem_Click(object sender, EventArgs e)
        {
            //Esto es para movernos entre formularios consultas
            Frm_Consultas consultas = new Frm_Consultas();
            consultas.Show();
            this.Hide();
        }

        private void Frm_Creacion_Load(object sender, EventArgs e)
        {

        }

        private void button1_Click(object sender, EventArgs e)
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

        private void lstCampos_SelectedIndexChanged(object sender, EventArgs e)
        {

        }
    }
}

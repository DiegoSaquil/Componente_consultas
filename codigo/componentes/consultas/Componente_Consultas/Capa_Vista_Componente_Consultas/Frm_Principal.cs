using System;
using System.Windows.Forms;

namespace Capa_Vista_Componente_Consultas
{

    // Realizado por: Nelson Jose Godinez Mendez 0901-22-3550
    public partial class Frm_Principal : Form
    {
        public Frm_Principal()
        {
            InitializeComponent();
        }

       
        private void btn_ConsultaSimple_Click(object sender, EventArgs e)
        {
            using (var f = new Frm_Consultas())
            {
                this.Hide();
                f.ShowDialog(this);   
                this.Show();
            }
        }

        
        private void btn_ConsultaCompleja_Click(object sender, EventArgs e)
        {
            using (var f = new Frm_Consulta_Compleja())
            {
                this.Hide();
                f.ShowDialog(this);
                this.Show();
            }
        }

       
        private void btn_Ayuda_Click(object sender, EventArgs e)
        {
            try
            {
                string url = "https://drive.google.com/drive/folders/1eOTejEnZx027FXOCr0ALDAdZhShsIidU?usp=sharing";
                System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
                {
                    FileName = url,
                    UseShellExecute = true
                });
            }
            catch (Exception ex)
            {
                MessageBox.Show("No se pudo abrir el manual en línea.\n" + ex.Message,
                                "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

       
        private void btn_Cerrar_Click(object sender, EventArgs e)
        {
            Application.Exit();
        }

        protected override void OnFormClosed(FormClosedEventArgs e)
        {
            base.OnFormClosed(e);
            Application.Exit();
        }
    }
}

using System;
using System.Windows.Forms;
using Capa_Vista_Componente_Consultas;

namespace Prototipo_Consultas
{
    static class Program
    {
        [STAThread]
        static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new Frm_Principal());
        }
    }
}

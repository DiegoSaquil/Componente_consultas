using System;
using System.Data;
using System.Data.Odbc;

namespace Capa_Modelo_Componente_Consultas
{
    // Juan Carlos Sandoval Quej 0901-22-4170
    public sealed class Conexion : IDisposable
    {
        private readonly string _dsn;
        private OdbcConnection _cn;
        #region Propiedades (estandar público "p")
        public string pDsn => _dsn;
        public OdbcConnection pConexion => _cn;
        #endregion


        public Conexion() : this("Prueba1") { }
        public Conexion(string dsn)
        {
            if (string.IsNullOrWhiteSpace(dsn))
                throw new ArgumentException("Debes especificar el nombre del DSN.", nameof(dsn));
            _dsn = dsn;
        }

        public OdbcConnection Abrir()
        {
            if (_cn == null)
            {
                _cn = new OdbcConnection();
                _cn.ConnectionString = $"Dsn={_dsn};"; // siempre inicializamos ConnectionString
            }

            if (_cn.State != ConnectionState.Open)
                _cn.Open();

            return _cn;
        }

        public void Dispose()
        {
            _cn?.Dispose();
            _cn = null;
        }

        // API académica (por si la piden)
        public OdbcConnection conexion() => Abrir();
        public void desconexion(OdbcConnection con)
        {
            try
            {
                if (con != null && con.State != ConnectionState.Closed)
                    con.Close();
            }
            catch { /* opcional log */ }
        }
    }
}


namespace Capa_Vista_Componente_Consultas
{
    partial class Frm_Creacion
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.gbAcciones = new System.Windows.Forms.GroupBox();
            this.btnCerrar = new System.Windows.Forms.Button();
            this.btnBorrar = new System.Windows.Forms.Button();
            this.btnCrear = new System.Windows.Forms.Button();
            this.txtSqlPreview = new System.Windows.Forms.TextBox();
            this.gbOrden = new System.Windows.Forms.GroupBox();
            this.lstOrden = new System.Windows.Forms.ListBox();
            this.btnQuitarOrden = new System.Windows.Forms.Button();
            this.btnAgregarOrden = new System.Windows.Forms.Button();
            this.rdbDesc = new System.Windows.Forms.RadioButton();
            this.rdbAsc = new System.Windows.Forms.RadioButton();
            this.cmbCampoOrden = new System.Windows.Forms.ComboBox();
            this.gbCompleja = new System.Windows.Forms.GroupBox();
            this.btnQuitarCond = new System.Windows.Forms.Button();
            this.btnAgregarCond = new System.Windows.Forms.Button();
            this.txtValorCond = new System.Windows.Forms.TextBox();
            this.cmbConectorCond = new System.Windows.Forms.ComboBox();
            this.cmbOperadorCond = new System.Windows.Forms.ComboBox();
            this.cmbCampoCond = new System.Windows.Forms.ComboBox();
            this.lstCondiciones = new System.Windows.Forms.ListBox();
            this.chkAgregarCondiciones = new System.Windows.Forms.CheckBox();
            this.gbSimple = new System.Windows.Forms.GroupBox();
            this.btnLimpiarCampos = new System.Windows.Forms.Button();
            this.btnQuitarCampo = new System.Windows.Forms.Button();
            this.lstCampos = new System.Windows.Forms.ListBox();
            this.btnAgregarCampo = new System.Windows.Forms.Button();
            this.cmbOperacionSimple = new System.Windows.Forms.ComboBox();
            this.cmbCampoSimple = new System.Windows.Forms.ComboBox();
            this.cmbTablaSimple = new System.Windows.Forms.ComboBox();
            this.Mstp_Consultas = new System.Windows.Forms.MenuStrip();
            this.creaciònToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.consultasToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editarToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.btn_min = new System.Windows.Forms.Button();
            this.btn_max = new System.Windows.Forms.Button();
            this.btn_cerrar = new System.Windows.Forms.Button();
            this.gbAcciones.SuspendLayout();
            this.gbOrden.SuspendLayout();
            this.gbCompleja.SuspendLayout();
            this.gbSimple.SuspendLayout();
            this.Mstp_Consultas.SuspendLayout();
            this.SuspendLayout();
            // 
            // gbAcciones
            // 
            this.gbAcciones.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.gbAcciones.Controls.Add(this.btnCerrar);
            this.gbAcciones.Controls.Add(this.btnBorrar);
            this.gbAcciones.Controls.Add(this.btnCrear);
            this.gbAcciones.Controls.Add(this.txtSqlPreview);
            this.gbAcciones.Location = new System.Drawing.Point(10, 486);
            this.gbAcciones.Name = "gbAcciones";
            this.gbAcciones.Size = new System.Drawing.Size(782, 120);
            this.gbAcciones.TabIndex = 7;
            this.gbAcciones.TabStop = false;
            this.gbAcciones.Text = "Acciones";
            // 
            // btnCerrar
            // 
            this.btnCerrar.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnCerrar.Location = new System.Drawing.Point(678, 86);
            this.btnCerrar.Name = "btnCerrar";
            this.btnCerrar.Size = new System.Drawing.Size(75, 23);
            this.btnCerrar.TabIndex = 3;
            this.btnCerrar.Text = "Cerrar";
            this.btnCerrar.UseVisualStyleBackColor = true;
            // 
            // btnBorrar
            // 
            this.btnBorrar.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnBorrar.Location = new System.Drawing.Point(597, 86);
            this.btnBorrar.Name = "btnBorrar";
            this.btnBorrar.Size = new System.Drawing.Size(75, 23);
            this.btnBorrar.TabIndex = 2;
            this.btnBorrar.Text = "Borrar";
            this.btnBorrar.UseVisualStyleBackColor = true;
            // 
            // btnCrear
            // 
            this.btnCrear.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnCrear.Location = new System.Drawing.Point(516, 86);
            this.btnCrear.Name = "btnCrear";
            this.btnCrear.Size = new System.Drawing.Size(75, 23);
            this.btnCrear.TabIndex = 1;
            this.btnCrear.Text = "Crear /";
            this.btnCrear.UseVisualStyleBackColor = true;
            // 
            // txtSqlPreview
            // 
            this.txtSqlPreview.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.txtSqlPreview.Location = new System.Drawing.Point(16, 25);
            this.txtSqlPreview.Multiline = true;
            this.txtSqlPreview.Name = "txtSqlPreview";
            this.txtSqlPreview.ReadOnly = true;
            this.txtSqlPreview.Size = new System.Drawing.Size(737, 55);
            this.txtSqlPreview.TabIndex = 0;
            // 
            // gbOrden
            // 
            this.gbOrden.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.gbOrden.Controls.Add(this.lstOrden);
            this.gbOrden.Controls.Add(this.btnQuitarOrden);
            this.gbOrden.Controls.Add(this.btnAgregarOrden);
            this.gbOrden.Controls.Add(this.rdbDesc);
            this.gbOrden.Controls.Add(this.rdbAsc);
            this.gbOrden.Controls.Add(this.cmbCampoOrden);
            this.gbOrden.Location = new System.Drawing.Point(10, 359);
            this.gbOrden.Name = "gbOrden";
            this.gbOrden.Size = new System.Drawing.Size(782, 120);
            this.gbOrden.TabIndex = 6;
            this.gbOrden.TabStop = false;
            this.gbOrden.Text = "Ordenar";
            // 
            // lstOrden
            // 
            this.lstOrden.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.lstOrden.FormattingEnabled = true;
            this.lstOrden.Location = new System.Drawing.Point(492, 19);
            this.lstOrden.Name = "lstOrden";
            this.lstOrden.Size = new System.Drawing.Size(261, 56);
            this.lstOrden.TabIndex = 5;
            // 
            // btnQuitarOrden
            // 
            this.btnQuitarOrden.Location = new System.Drawing.Point(575, 81);
            this.btnQuitarOrden.Name = "btnQuitarOrden";
            this.btnQuitarOrden.Size = new System.Drawing.Size(75, 23);
            this.btnQuitarOrden.TabIndex = 4;
            this.btnQuitarOrden.Text = "Quitar";
            this.btnQuitarOrden.UseVisualStyleBackColor = true;
            // 
            // btnAgregarOrden
            // 
            this.btnAgregarOrden.Location = new System.Drawing.Point(494, 81);
            this.btnAgregarOrden.Name = "btnAgregarOrden";
            this.btnAgregarOrden.Size = new System.Drawing.Size(75, 23);
            this.btnAgregarOrden.TabIndex = 3;
            this.btnAgregarOrden.Text = "Agregar";
            this.btnAgregarOrden.UseVisualStyleBackColor = true;
            // 
            // rdbDesc
            // 
            this.rdbDesc.AutoSize = true;
            this.rdbDesc.Location = new System.Drawing.Point(243, 84);
            this.rdbDesc.Name = "rdbDesc";
            this.rdbDesc.Size = new System.Drawing.Size(47, 17);
            this.rdbDesc.TabIndex = 2;
            this.rdbDesc.Text = "DES";
            this.rdbDesc.UseVisualStyleBackColor = true;
            // 
            // rdbAsc
            // 
            this.rdbAsc.AutoSize = true;
            this.rdbAsc.Checked = true;
            this.rdbAsc.Location = new System.Drawing.Point(193, 84);
            this.rdbAsc.Name = "rdbAsc";
            this.rdbAsc.Size = new System.Drawing.Size(46, 17);
            this.rdbAsc.TabIndex = 1;
            this.rdbAsc.TabStop = true;
            this.rdbAsc.Text = "ASC";
            this.rdbAsc.UseVisualStyleBackColor = true;
            // 
            // cmbCampoOrden
            // 
            this.cmbCampoOrden.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.cmbCampoOrden.FormattingEnabled = true;
            this.cmbCampoOrden.Location = new System.Drawing.Point(16, 82);
            this.cmbCampoOrden.Name = "cmbCampoOrden";
            this.cmbCampoOrden.Size = new System.Drawing.Size(171, 21);
            this.cmbCampoOrden.TabIndex = 0;
            // 
            // gbCompleja
            // 
            this.gbCompleja.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.gbCompleja.Controls.Add(this.btnQuitarCond);
            this.gbCompleja.Controls.Add(this.btnAgregarCond);
            this.gbCompleja.Controls.Add(this.txtValorCond);
            this.gbCompleja.Controls.Add(this.cmbConectorCond);
            this.gbCompleja.Controls.Add(this.cmbOperadorCond);
            this.gbCompleja.Controls.Add(this.cmbCampoCond);
            this.gbCompleja.Controls.Add(this.lstCondiciones);
            this.gbCompleja.Controls.Add(this.chkAgregarCondiciones);
            this.gbCompleja.Location = new System.Drawing.Point(10, 193);
            this.gbCompleja.Name = "gbCompleja";
            this.gbCompleja.Size = new System.Drawing.Size(782, 160);
            this.gbCompleja.TabIndex = 5;
            this.gbCompleja.TabStop = false;
            this.gbCompleja.Text = "Consulta Compleja";
            // 
            // btnQuitarCond
            // 
            this.btnQuitarCond.Location = new System.Drawing.Point(575, 123);
            this.btnQuitarCond.Name = "btnQuitarCond";
            this.btnQuitarCond.Size = new System.Drawing.Size(75, 23);
            this.btnQuitarCond.TabIndex = 8;
            this.btnQuitarCond.Text = "Quitar";
            this.btnQuitarCond.UseVisualStyleBackColor = true;
            // 
            // btnAgregarCond
            // 
            this.btnAgregarCond.Location = new System.Drawing.Point(494, 123);
            this.btnAgregarCond.Name = "btnAgregarCond";
            this.btnAgregarCond.Size = new System.Drawing.Size(75, 23);
            this.btnAgregarCond.TabIndex = 7;
            this.btnAgregarCond.Text = "Agregar";
            this.btnAgregarCond.UseVisualStyleBackColor = true;
            // 
            // txtValorCond
            // 
            this.txtValorCond.Location = new System.Drawing.Point(304, 123);
            this.txtValorCond.Name = "txtValorCond";
            this.txtValorCond.Size = new System.Drawing.Size(184, 20);
            this.txtValorCond.TabIndex = 6;
            // 
            // cmbConectorCond
            // 
            this.cmbConectorCond.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.cmbConectorCond.FormattingEnabled = true;
            this.cmbConectorCond.Items.AddRange(new object[] {
            "AND",
            "OR"});
            this.cmbConectorCond.Location = new System.Drawing.Point(16, 122);
            this.cmbConectorCond.Name = "cmbConectorCond";
            this.cmbConectorCond.Size = new System.Drawing.Size(92, 21);
            this.cmbConectorCond.TabIndex = 5;
            // 
            // cmbOperadorCond
            // 
            this.cmbOperadorCond.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.cmbOperadorCond.FormattingEnabled = true;
            this.cmbOperadorCond.Items.AddRange(new object[] {
            "=",
            "<>",
            ">",
            "<",
            ">=",
            "<="});
            this.cmbOperadorCond.Location = new System.Drawing.Point(114, 122);
            this.cmbOperadorCond.Name = "cmbOperadorCond";
            this.cmbOperadorCond.Size = new System.Drawing.Size(84, 21);
            this.cmbOperadorCond.TabIndex = 4;
            // 
            // cmbCampoCond
            // 
            this.cmbCampoCond.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.cmbCampoCond.FormattingEnabled = true;
            this.cmbCampoCond.Location = new System.Drawing.Point(204, 122);
            this.cmbCampoCond.Name = "cmbCampoCond";
            this.cmbCampoCond.Size = new System.Drawing.Size(94, 21);
            this.cmbCampoCond.TabIndex = 3;
            // 
            // lstCondiciones
            // 
            this.lstCondiciones.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.lstCondiciones.FormattingEnabled = true;
            this.lstCondiciones.Location = new System.Drawing.Point(492, 19);
            this.lstCondiciones.Name = "lstCondiciones";
            this.lstCondiciones.Size = new System.Drawing.Size(261, 95);
            this.lstCondiciones.TabIndex = 2;
            // 
            // chkAgregarCondiciones
            // 
            this.chkAgregarCondiciones.AutoSize = true;
            this.chkAgregarCondiciones.Location = new System.Drawing.Point(16, 25);
            this.chkAgregarCondiciones.Name = "chkAgregarCondiciones";
            this.chkAgregarCondiciones.Size = new System.Drawing.Size(63, 17);
            this.chkAgregarCondiciones.TabIndex = 0;
            this.chkAgregarCondiciones.Text = "Agregar";
            this.chkAgregarCondiciones.UseVisualStyleBackColor = true;
            // 
            // gbSimple
            // 
            this.gbSimple.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.gbSimple.Controls.Add(this.btnLimpiarCampos);
            this.gbSimple.Controls.Add(this.btnQuitarCampo);
            this.gbSimple.Controls.Add(this.lstCampos);
            this.gbSimple.Controls.Add(this.btnAgregarCampo);
            this.gbSimple.Controls.Add(this.cmbOperacionSimple);
            this.gbSimple.Controls.Add(this.cmbCampoSimple);
            this.gbSimple.Controls.Add(this.cmbTablaSimple);
            this.gbSimple.Location = new System.Drawing.Point(10, 36);
            this.gbSimple.Name = "gbSimple";
            this.gbSimple.Size = new System.Drawing.Size(782, 150);
            this.gbSimple.TabIndex = 4;
            this.gbSimple.TabStop = false;
            this.gbSimple.Text = "Consulta simple";
            // 
            // btnLimpiarCampos
            // 
            this.btnLimpiarCampos.Location = new System.Drawing.Point(656, 106);
            this.btnLimpiarCampos.Name = "btnLimpiarCampos";
            this.btnLimpiarCampos.Size = new System.Drawing.Size(75, 23);
            this.btnLimpiarCampos.TabIndex = 6;
            this.btnLimpiarCampos.Text = "Limpiar";
            this.btnLimpiarCampos.UseVisualStyleBackColor = true;
            // 
            // btnQuitarCampo
            // 
            this.btnQuitarCampo.Location = new System.Drawing.Point(575, 106);
            this.btnQuitarCampo.Name = "btnQuitarCampo";
            this.btnQuitarCampo.Size = new System.Drawing.Size(75, 23);
            this.btnQuitarCampo.TabIndex = 5;
            this.btnQuitarCampo.Text = "Quitar";
            this.btnQuitarCampo.UseVisualStyleBackColor = true;
            // 
            // lstCampos
            // 
            this.lstCampos.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.lstCampos.FormattingEnabled = true;
            this.lstCampos.Location = new System.Drawing.Point(492, 19);
            this.lstCampos.Name = "lstCampos";
            this.lstCampos.Size = new System.Drawing.Size(261, 82);
            this.lstCampos.TabIndex = 4;
            this.lstCampos.SelectedIndexChanged += new System.EventHandler(this.lstCampos_SelectedIndexChanged);
            // 
            // btnAgregarCampo
            // 
            this.btnAgregarCampo.Location = new System.Drawing.Point(374, 77);
            this.btnAgregarCampo.Name = "btnAgregarCampo";
            this.btnAgregarCampo.Size = new System.Drawing.Size(75, 23);
            this.btnAgregarCampo.TabIndex = 3;
            this.btnAgregarCampo.Text = "Agregar";
            this.btnAgregarCampo.UseVisualStyleBackColor = true;
            // 
            // cmbOperacionSimple
            // 
            this.cmbOperacionSimple.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.cmbOperacionSimple.FormattingEnabled = true;
            this.cmbOperacionSimple.Items.AddRange(new object[] {
            "=",
            "<>",
            ">",
            "<",
            ">=",
            "<="});
            this.cmbOperacionSimple.Location = new System.Drawing.Point(16, 103);
            this.cmbOperacionSimple.Name = "cmbOperacionSimple";
            this.cmbOperacionSimple.Size = new System.Drawing.Size(161, 21);
            this.cmbOperacionSimple.TabIndex = 2;
            // 
            // cmbCampoSimple
            // 
            this.cmbCampoSimple.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.cmbCampoSimple.FormattingEnabled = true;
            this.cmbCampoSimple.Location = new System.Drawing.Point(16, 63);
            this.cmbCampoSimple.Name = "cmbCampoSimple";
            this.cmbCampoSimple.Size = new System.Drawing.Size(352, 21);
            this.cmbCampoSimple.TabIndex = 1;
            // 
            // cmbTablaSimple
            // 
            this.cmbTablaSimple.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.cmbTablaSimple.FormattingEnabled = true;
            this.cmbTablaSimple.Location = new System.Drawing.Point(16, 25);
            this.cmbTablaSimple.Name = "cmbTablaSimple";
            this.cmbTablaSimple.Size = new System.Drawing.Size(352, 21);
            this.cmbTablaSimple.TabIndex = 0;
            // 
            // Mstp_Consultas
            // 
            this.Mstp_Consultas.ImageScalingSize = new System.Drawing.Size(20, 20);
            this.Mstp_Consultas.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.creaciònToolStripMenuItem,
            this.consultasToolStripMenuItem,
            this.editarToolStripMenuItem});
            this.Mstp_Consultas.Location = new System.Drawing.Point(0, 0);
            this.Mstp_Consultas.Name = "Mstp_Consultas";
            this.Mstp_Consultas.Padding = new System.Windows.Forms.Padding(4, 2, 0, 2);
            this.Mstp_Consultas.Size = new System.Drawing.Size(806, 25);
            this.Mstp_Consultas.TabIndex = 8;
            this.Mstp_Consultas.Text = "menuStrip1";
            // 
            // creaciònToolStripMenuItem
            // 
            this.creaciònToolStripMenuItem.Font = new System.Drawing.Font("Rockwell", 10.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.creaciònToolStripMenuItem.Name = "creaciònToolStripMenuItem";
            this.creaciònToolStripMenuItem.Size = new System.Drawing.Size(79, 21);
            this.creaciònToolStripMenuItem.Text = "Creación";
            // 
            // consultasToolStripMenuItem
            // 
            this.consultasToolStripMenuItem.Font = new System.Drawing.Font("Rockwell", 10.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.consultasToolStripMenuItem.Name = "consultasToolStripMenuItem";
            this.consultasToolStripMenuItem.Size = new System.Drawing.Size(82, 21);
            this.consultasToolStripMenuItem.Text = "Consultas";
            this.consultasToolStripMenuItem.Click += new System.EventHandler(this.consultasToolStripMenuItem_Click);
            // 
            // editarToolStripMenuItem
            // 
            this.editarToolStripMenuItem.Font = new System.Drawing.Font("Rockwell", 10.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.editarToolStripMenuItem.Name = "editarToolStripMenuItem";
            this.editarToolStripMenuItem.Size = new System.Drawing.Size(119, 21);
            this.editarToolStripMenuItem.Text = "Editar/Eliminar";
            this.editarToolStripMenuItem.Click += new System.EventHandler(this.editarToolStripMenuItem_Click);
            // 
            // btn_min
            // 
            this.btn_min.Location = new System.Drawing.Point(661, 0);
            this.btn_min.Name = "btn_min";
            this.btn_min.Size = new System.Drawing.Size(64, 23);
            this.btn_min.TabIndex = 10;
            this.btn_min.Text = "Minimizar";
            this.btn_min.UseVisualStyleBackColor = true;
            this.btn_min.Click += new System.EventHandler(this.btn_min_Click);
            // 
            // btn_max
            // 
            this.btn_max.Location = new System.Drawing.Point(731, 0);
            this.btn_max.Name = "btn_max";
            this.btn_max.Size = new System.Drawing.Size(63, 23);
            this.btn_max.TabIndex = 11;
            this.btn_max.Text = "Maximizar";
            this.btn_max.UseVisualStyleBackColor = true;
            this.btn_max.Click += new System.EventHandler(this.btn_max_Click);
            // 
            // btn_cerrar
            // 
            this.btn_cerrar.Location = new System.Drawing.Point(607, 0);
            this.btn_cerrar.Name = "btn_cerrar";
            this.btn_cerrar.Size = new System.Drawing.Size(48, 23);
            this.btn_cerrar.TabIndex = 12;
            this.btn_cerrar.Text = "Cerrar";
            this.btn_cerrar.UseVisualStyleBackColor = true;
            this.btn_cerrar.Click += new System.EventHandler(this.btn_cerrar_Click);
            // 
            // Frm_Creacion
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(806, 609);
            this.Controls.Add(this.btn_cerrar);
            this.Controls.Add(this.btn_max);
            this.Controls.Add(this.btn_min);
            this.Controls.Add(this.gbAcciones);
            this.Controls.Add(this.gbOrden);
            this.Controls.Add(this.gbCompleja);
            this.Controls.Add(this.gbSimple);
            this.Controls.Add(this.Mstp_Consultas);
            this.MainMenuStrip = this.Mstp_Consultas;
            this.Margin = new System.Windows.Forms.Padding(2);
            this.MinimumSize = new System.Drawing.Size(800, 591);
            this.Name = "Frm_Creacion";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "FrmConsultas";
            this.Load += new System.EventHandler(this.Frm_Creacion_Load);
            this.gbAcciones.ResumeLayout(false);
            this.gbAcciones.PerformLayout();
            this.gbOrden.ResumeLayout(false);
            this.gbOrden.PerformLayout();
            this.gbCompleja.ResumeLayout(false);
            this.gbCompleja.PerformLayout();
            this.gbSimple.ResumeLayout(false);
            this.Mstp_Consultas.ResumeLayout(false);
            this.Mstp_Consultas.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.GroupBox gbAcciones;
        private System.Windows.Forms.Button btnCerrar;
        private System.Windows.Forms.Button btnBorrar;
        private System.Windows.Forms.Button btnCrear;
        private System.Windows.Forms.TextBox txtSqlPreview;
        private System.Windows.Forms.GroupBox gbOrden;
        private System.Windows.Forms.ListBox lstOrden;
        private System.Windows.Forms.Button btnQuitarOrden;
        private System.Windows.Forms.Button btnAgregarOrden;
        private System.Windows.Forms.RadioButton rdbDesc;
        private System.Windows.Forms.RadioButton rdbAsc;
        private System.Windows.Forms.ComboBox cmbCampoOrden;
        private System.Windows.Forms.GroupBox gbCompleja;
        private System.Windows.Forms.Button btnQuitarCond;
        private System.Windows.Forms.Button btnAgregarCond;
        private System.Windows.Forms.TextBox txtValorCond;
        private System.Windows.Forms.ComboBox cmbConectorCond;
        private System.Windows.Forms.ComboBox cmbOperadorCond;
        private System.Windows.Forms.ComboBox cmbCampoCond;
        private System.Windows.Forms.ListBox lstCondiciones;
        private System.Windows.Forms.CheckBox chkAgregarCondiciones;
        private System.Windows.Forms.GroupBox gbSimple;
        private System.Windows.Forms.Button btnLimpiarCampos;
        private System.Windows.Forms.Button btnQuitarCampo;
        private System.Windows.Forms.ListBox lstCampos;
        private System.Windows.Forms.Button btnAgregarCampo;
        private System.Windows.Forms.ComboBox cmbOperacionSimple;
        private System.Windows.Forms.ComboBox cmbCampoSimple;
        private System.Windows.Forms.ComboBox cmbTablaSimple;
        private System.Windows.Forms.MenuStrip Mstp_Consultas;
        private System.Windows.Forms.ToolStripMenuItem creaciònToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem consultasToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem editarToolStripMenuItem;
        private System.Windows.Forms.Button btn_min;
        private System.Windows.Forms.Button btn_max;
        private System.Windows.Forms.Button btn_cerrar;
    }
}
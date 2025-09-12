namespace Capa_Vista_Componente_Consultas
{
    partial class Frm_Eliminar
    {
        private System.ComponentModel.IContainer components = null;

        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Código generado por el Diseñador de Windows Forms

        private void InitializeComponent()
        {
            this.menuStrip1 = new System.Windows.Forms.MenuStrip();
            this.creacionToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.consultasToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editarToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.buscarEliminarToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.lblNombreConsulta = new System.Windows.Forms.Label();
            this.txtNombreConsulta = new System.Windows.Forms.TextBox();
            this.btn_buscar = new System.Windows.Forms.Button();
            this.btn_actualizar = new System.Windows.Forms.Button();
            this.btn_eliminar = new System.Windows.Forms.Button();
            this.btn_consultas = new System.Windows.Forms.Button();
            this.dgvConsultas = new System.Windows.Forms.DataGridView();
            this.btn_cerrar = new System.Windows.Forms.Button();
            this.btn_min = new System.Windows.Forms.Button();
            this.btn_max = new System.Windows.Forms.Button();
            this.menuStrip1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dgvConsultas)).BeginInit();
            this.SuspendLayout();
            // 
            // menuStrip1
            // 
            this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.creacionToolStripMenuItem,
            this.consultasToolStripMenuItem,
            this.editarToolStripMenuItem,
            this.buscarEliminarToolStripMenuItem});
            this.menuStrip1.Location = new System.Drawing.Point(0, 0);
            this.menuStrip1.Name = "menuStrip1";
            this.menuStrip1.Size = new System.Drawing.Size(900, 24);
            this.menuStrip1.TabIndex = 0;
            this.menuStrip1.Text = "menuStrip1";
            // 
            // creacionToolStripMenuItem
            // 
            this.creacionToolStripMenuItem.Name = "creacionToolStripMenuItem";
            this.creacionToolStripMenuItem.Size = new System.Drawing.Size(68, 20);
            this.creacionToolStripMenuItem.Text = "CREACION";
            this.creacionToolStripMenuItem.Click += new System.EventHandler(this.creacionToolStripMenuItem_Click);
            // 
            // consultasToolStripMenuItem
            // 
            this.consultasToolStripMenuItem.Name = "consultasToolStripMenuItem";
            this.consultasToolStripMenuItem.Size = new System.Drawing.Size(79, 20);
            this.consultasToolStripMenuItem.Text = "CONSULTAS";
            this.consultasToolStripMenuItem.Click += new System.EventHandler(this.consultasToolStripMenuItem_Click);
            // 
            // editarToolStripMenuItem
            // 
            this.editarToolStripMenuItem.Name = "editarToolStripMenuItem";
            this.editarToolStripMenuItem.Size = new System.Drawing.Size(53, 20);
            this.editarToolStripMenuItem.Text = "EDITAR";
            // 
            // buscarEliminarToolStripMenuItem
            // 
            this.buscarEliminarToolStripMenuItem.Name = "buscarEliminarToolStripMenuItem";
            this.buscarEliminarToolStripMenuItem.Size = new System.Drawing.Size(111, 20);
            this.buscarEliminarToolStripMenuItem.Text = "BUSCAR/ELIMINAR";
            // 
            // lblNombreConsulta
            // 
            this.lblNombreConsulta.AutoSize = true;
            this.lblNombreConsulta.Location = new System.Drawing.Point(25, 50);
            this.lblNombreConsulta.Name = "lblNombreConsulta";
            this.lblNombreConsulta.Size = new System.Drawing.Size(94, 13);
            this.lblNombreConsulta.TabIndex = 1;
            this.lblNombreConsulta.Text = "Nombre Consulta:";
            // 
            // txtNombreConsulta
            // 
            this.txtNombreConsulta.Location = new System.Drawing.Point(125, 47);
            this.txtNombreConsulta.Name = "txtNombreConsulta";
            this.txtNombreConsulta.Size = new System.Drawing.Size(200, 20);
            this.txtNombreConsulta.TabIndex = 2;
            // 
            // btn_buscar
            // 
            this.btn_buscar.Location = new System.Drawing.Point(350, 40);
            this.btn_buscar.Name = "btn_buscar";
            this.btn_buscar.Size = new System.Drawing.Size(100, 35);
            this.btn_buscar.TabIndex = 3;
            this.btn_buscar.Text = "BUSCAR";
            this.btn_buscar.UseVisualStyleBackColor = true;
            this.btn_buscar.Click += new System.EventHandler(this.btn_buscar_Click);
            // 
            // btn_actualizar
            // 
            this.btn_actualizar.Location = new System.Drawing.Point(470, 40);
            this.btn_actualizar.Name = "btn_actualizar";
            this.btn_actualizar.Size = new System.Drawing.Size(100, 35);
            this.btn_actualizar.TabIndex = 4;
            this.btn_actualizar.Text = "ACTUALIZAR";
            this.btn_actualizar.UseVisualStyleBackColor = true;
            this.btn_actualizar.Click += new System.EventHandler(this.btn_actualizar_Click);
            // 
            // btn_eliminar
            // 
            this.btn_eliminar.Location = new System.Drawing.Point(590, 40);
            this.btn_eliminar.Name = "btn_eliminar";
            this.btn_eliminar.Size = new System.Drawing.Size(100, 35);
            this.btn_eliminar.TabIndex = 5;
            this.btn_eliminar.Text = "ELIMINAR";
            this.btn_eliminar.UseVisualStyleBackColor = true;
            this.btn_eliminar.Click += new System.EventHandler(this.btn_eliminar_Click);
            // 
            // btn_consultas
            // 
            this.btn_consultas.Location = new System.Drawing.Point(710, 40);
            this.btn_consultas.Name = "btn_consultas";
            this.btn_consultas.Size = new System.Drawing.Size(100, 35);
            this.btn_consultas.TabIndex = 6;
            this.btn_consultas.Text = "CONSULTAS";
            this.btn_consultas.UseVisualStyleBackColor = true;
            this.btn_consultas.Click += new System.EventHandler(this.btn_consultas_Click);
            // 
            // dgvConsultas
            // 
            this.dgvConsultas.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dgvConsultas.Location = new System.Drawing.Point(28, 100);
            this.dgvConsultas.Name = "dgvConsultas";
            this.dgvConsultas.Size = new System.Drawing.Size(820, 250);
            this.dgvConsultas.TabIndex = 7;
            // 
            // btn_cerrar
            // 
            this.btn_cerrar.Location = new System.Drawing.Point(860, 0);
            this.btn_cerrar.Name = "btn_cerrar";
            this.btn_cerrar.Size = new System.Drawing.Size(40, 23);
            this.btn_cerrar.TabIndex = 8;
            this.btn_cerrar.Text = "X";
            this.btn_cerrar.UseVisualStyleBackColor = true;
            this.btn_cerrar.Click += new System.EventHandler(this.btn_cerrar_Click);
            // 
            // btn_min
            // 
            this.btn_min.Location = new System.Drawing.Point(780, 0);
            this.btn_min.Name = "btn_min";
            this.btn_min.Size = new System.Drawing.Size(40, 23);
            this.btn_min.TabIndex = 9;
            this.btn_min.Text = "__";
            this.btn_min.UseVisualStyleBackColor = true;
            this.btn_min.Click += new System.EventHandler(this.btn_min_Click);
            // 
            // btn_max
            // 
            this.btn_max.Location = new System.Drawing.Point(820, 0);
            this.btn_max.Name = "btn_max";
            this.btn_max.Size = new System.Drawing.Size(40, 23);
            this.btn_max.TabIndex = 10;
            this.btn_max.Text = "[]";
            this.btn_max.UseVisualStyleBackColor = true;
            this.btn_max.Click += new System.EventHandler(this.btn_max_Click);
            // 
            // Frm_Eliminar
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(900, 400);
            this.Controls.Add(this.btn_max);
            this.Controls.Add(this.btn_min);
            this.Controls.Add(this.btn_cerrar);
            this.Controls.Add(this.dgvConsultas);
            this.Controls.Add(this.btn_consultas);
            this.Controls.Add(this.btn_eliminar);
            this.Controls.Add(this.btn_actualizar);
            this.Controls.Add(this.btn_buscar);
            this.Controls.Add(this.txtNombreConsulta);
            this.Controls.Add(this.lblNombreConsulta);
            this.Controls.Add(this.menuStrip1);
            this.MainMenuStrip = this.menuStrip1;
            this.Name = "Frm_Eliminar";
            this.Text = "Frm_Eliminar";
            this.Load += new System.EventHandler(this.Frm_Eliminar_Load);
            this.menuStrip1.ResumeLayout(false);
            this.menuStrip1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dgvConsultas)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();
        }

        #endregion

        private System.Windows.Forms.MenuStrip menuStrip1;
        private System.Windows.Forms.ToolStripMenuItem creacionToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem consultasToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem editarToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem buscarEliminarToolStripMenuItem;
        private System.Windows.Forms.Label lblNombreConsulta;
        private System.Windows.Forms.TextBox txtNombreConsulta;
        private System.Windows.Forms.Button btn_buscar;
        private System.Windows.Forms.Button btn_actualizar;
        private System.Windows.Forms.Button btn_eliminar;
        private System.Windows.Forms.Button btn_consultas;
        private System.Windows.Forms.DataGridView dgvConsultas;
        private System.Windows.Forms.Button btn_cerrar;
        private System.Windows.Forms.Button btn_min;
        private System.Windows.Forms.Button btn_max;
    }
}

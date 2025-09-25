
namespace Capa_Vista_Componente_Consultas
{
    partial class Frm_Consulta_Compleja
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
            this.Cbo_busqueda = new System.Windows.Forms.ComboBox();
            this.Lbl_Label1 = new System.Windows.Forms.Label();
            this.Dgv_Tabla = new System.Windows.Forms.DataGridView();
            this.Btn_editar = new System.Windows.Forms.Button();
            this.Btn_eliminar = new System.Windows.Forms.Button();
            this.Btn_EjecutarSQL = new System.Windows.Forms.Button();
            this.Rdb_asc = new System.Windows.Forms.RadioButton();
            this.Rdb_desc = new System.Windows.Forms.RadioButton();
            this.Btn_regresar = new System.Windows.Forms.Button();
            this.Btn_consimple = new System.Windows.Forms.Button();
            this.Lbl_Tabla = new System.Windows.Forms.Label();
            this.Lst_Querys = new System.Windows.Forms.ListBox();
            this.Gpb_Orden = new System.Windows.Forms.GroupBox();
            this.Lbl_Sql = new System.Windows.Forms.Label();
            this.Txt_Sql = new System.Windows.Forms.TextBox();
            this.Dgv_Sql = new System.Windows.Forms.DataGridView();
            this.Btn_buscar = new System.Windows.Forms.Button();
            this.Btn_min = new System.Windows.Forms.Button();
            this.Btn_max = new System.Windows.Forms.Button();
            ((System.ComponentModel.ISupportInitialize)(this.Dgv_Tabla)).BeginInit();
            this.Gpb_Orden.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.Dgv_Sql)).BeginInit();
            this.SuspendLayout();
            // 
            // Cbo_busqueda
            // 
            this.Cbo_busqueda.Font = new System.Drawing.Font("Rockwell", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Cbo_busqueda.FormattingEnabled = true;
            this.Cbo_busqueda.Location = new System.Drawing.Point(288, 33);
            this.Cbo_busqueda.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Cbo_busqueda.Name = "Cbo_busqueda";
            this.Cbo_busqueda.Size = new System.Drawing.Size(299, 27);
            this.Cbo_busqueda.TabIndex = 0;
            // 
            // Lbl_Label1
            // 
            this.Lbl_Label1.AutoSize = true;
            this.Lbl_Label1.Location = new System.Drawing.Point(397, 98);
            this.Lbl_Label1.Name = "Lbl_Label1";
            this.Lbl_Label1.Size = new System.Drawing.Size(0, 17);
            this.Lbl_Label1.TabIndex = 1;
            // 
            // Dgv_Tabla
            // 
            this.Dgv_Tabla.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.Dgv_Tabla.Location = new System.Drawing.Point(27, 94);
            this.Dgv_Tabla.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Dgv_Tabla.Name = "Dgv_Tabla";
            this.Dgv_Tabla.RowHeadersWidth = 51;
            this.Dgv_Tabla.RowTemplate.Height = 24;
            this.Dgv_Tabla.Size = new System.Drawing.Size(988, 274);
            this.Dgv_Tabla.TabIndex = 7;
            // 
            // Btn_editar
            // 
            this.Btn_editar.Font = new System.Drawing.Font("Rockwell", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Btn_editar.Location = new System.Drawing.Point(1101, 415);
            this.Btn_editar.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Btn_editar.Name = "Btn_editar";
            this.Btn_editar.Size = new System.Drawing.Size(89, 32);
            this.Btn_editar.TabIndex = 8;
            this.Btn_editar.Text = "Editar";
            this.Btn_editar.UseVisualStyleBackColor = true;
            this.Btn_editar.Click += new System.EventHandler(this.Btn_editar_Click);
            // 
            // Btn_eliminar
            // 
            this.Btn_eliminar.Font = new System.Drawing.Font("Rockwell", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Btn_eliminar.Location = new System.Drawing.Point(1229, 417);
            this.Btn_eliminar.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Btn_eliminar.Name = "Btn_eliminar";
            this.Btn_eliminar.Size = new System.Drawing.Size(105, 30);
            this.Btn_eliminar.TabIndex = 9;
            this.Btn_eliminar.Text = "Eliminar";
            this.Btn_eliminar.UseVisualStyleBackColor = true;
            this.Btn_eliminar.Click += new System.EventHandler(this.Btn_eliminar_Click);
            // 
            // Btn_EjecutarSQL
            // 
            this.Btn_EjecutarSQL.Font = new System.Drawing.Font("Rockwell", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Btn_EjecutarSQL.Location = new System.Drawing.Point(938, 423);
            this.Btn_EjecutarSQL.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Btn_EjecutarSQL.Name = "Btn_EjecutarSQL";
            this.Btn_EjecutarSQL.Size = new System.Drawing.Size(148, 70);
            this.Btn_EjecutarSQL.TabIndex = 10;
            this.Btn_EjecutarSQL.Text = "Ejecutar SQL";
            this.Btn_EjecutarSQL.UseVisualStyleBackColor = true;
            this.Btn_EjecutarSQL.Click += new System.EventHandler(this.Btn_EjecutarSQL_Click);
            // 
            // Rdb_asc
            // 
            this.Rdb_asc.AutoSize = true;
            this.Rdb_asc.Font = new System.Drawing.Font("Rockwell", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Rdb_asc.Location = new System.Drawing.Point(23, 32);
            this.Rdb_asc.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Rdb_asc.Name = "Rdb_asc";
            this.Rdb_asc.Size = new System.Drawing.Size(68, 25);
            this.Rdb_asc.TabIndex = 11;
            this.Rdb_asc.TabStop = true;
            this.Rdb_asc.Text = "ASC";
            this.Rdb_asc.UseVisualStyleBackColor = true;
            // 
            // Rdb_desc
            // 
            this.Rdb_desc.AutoSize = true;
            this.Rdb_desc.Font = new System.Drawing.Font("Rockwell", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Rdb_desc.Location = new System.Drawing.Point(132, 33);
            this.Rdb_desc.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Rdb_desc.Name = "Rdb_desc";
            this.Rdb_desc.Size = new System.Drawing.Size(81, 25);
            this.Rdb_desc.TabIndex = 12;
            this.Rdb_desc.TabStop = true;
            this.Rdb_desc.Text = "DESC";
            this.Rdb_desc.UseVisualStyleBackColor = true;
            // 
            // Btn_regresar
            // 
            this.Btn_regresar.Font = new System.Drawing.Font("Rockwell", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Btn_regresar.Location = new System.Drawing.Point(991, 7);
            this.Btn_regresar.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Btn_regresar.Name = "Btn_regresar";
            this.Btn_regresar.Size = new System.Drawing.Size(104, 31);
            this.Btn_regresar.TabIndex = 14;
            this.Btn_regresar.Text = "Regresar";
            this.Btn_regresar.UseVisualStyleBackColor = true;
            this.Btn_regresar.Click += new System.EventHandler(this.Btn_regresar_Click);
            // 
            // Btn_consimple
            // 
            this.Btn_consimple.Font = new System.Drawing.Font("Rockwell", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Btn_consimple.Location = new System.Drawing.Point(1100, 42);
            this.Btn_consimple.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Btn_consimple.Name = "Btn_consimple";
            this.Btn_consimple.Size = new System.Drawing.Size(133, 28);
            this.Btn_consimple.TabIndex = 15;
            this.Btn_consimple.Text = "Consulta Simple";
            this.Btn_consimple.UseVisualStyleBackColor = true;
            this.Btn_consimple.Click += new System.EventHandler(this.Btn_consimple_Click);
            // 
            // Lbl_Tabla
            // 
            this.Lbl_Tabla.AutoSize = true;
            this.Lbl_Tabla.Font = new System.Drawing.Font("Rockwell", 18F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lbl_Tabla.Location = new System.Drawing.Point(13, 30);
            this.Lbl_Tabla.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.Lbl_Tabla.Name = "Lbl_Tabla";
            this.Lbl_Tabla.Size = new System.Drawing.Size(250, 35);
            this.Lbl_Tabla.TabIndex = 17;
            this.Lbl_Tabla.Text = "Nombre de tabla";
            // 
            // Lst_Querys
            // 
            this.Lst_Querys.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.Lst_Querys.Font = new System.Drawing.Font("Rockwell", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lst_Querys.FormattingEnabled = true;
            this.Lst_Querys.ItemHeight = 19;
            this.Lst_Querys.Location = new System.Drawing.Point(1119, 94);
            this.Lst_Querys.Margin = new System.Windows.Forms.Padding(4);
            this.Lst_Querys.Name = "Lst_Querys";
            this.Lst_Querys.Size = new System.Drawing.Size(292, 289);
            this.Lst_Querys.TabIndex = 18;
            // 
            // Gpb_Orden
            // 
            this.Gpb_Orden.Controls.Add(this.Rdb_asc);
            this.Gpb_Orden.Controls.Add(this.Rdb_desc);
            this.Gpb_Orden.Font = new System.Drawing.Font("Rockwell", 18F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Gpb_Orden.Location = new System.Drawing.Point(731, 15);
            this.Gpb_Orden.Margin = new System.Windows.Forms.Padding(4);
            this.Gpb_Orden.Name = "Gpb_Orden";
            this.Gpb_Orden.Padding = new System.Windows.Forms.Padding(4);
            this.Gpb_Orden.Size = new System.Drawing.Size(212, 71);
            this.Gpb_Orden.TabIndex = 19;
            this.Gpb_Orden.TabStop = false;
            this.Gpb_Orden.Text = "Orden";
            // 
            // Lbl_Sql
            // 
            this.Lbl_Sql.AutoSize = true;
            this.Lbl_Sql.Font = new System.Drawing.Font("Rockwell", 18F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lbl_Sql.Location = new System.Drawing.Point(20, 377);
            this.Lbl_Sql.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.Lbl_Sql.Name = "Lbl_Sql";
            this.Lbl_Sql.Size = new System.Drawing.Size(245, 35);
            this.Lbl_Sql.TabIndex = 20;
            this.Lbl_Sql.Text = "Consola de SQL:";
            // 
            // Txt_Sql
            // 
            this.Txt_Sql.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.Txt_Sql.Location = new System.Drawing.Point(13, 423);
            this.Txt_Sql.Margin = new System.Windows.Forms.Padding(4);
            this.Txt_Sql.Multiline = true;
            this.Txt_Sql.Name = "Txt_Sql";
            this.Txt_Sql.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.Txt_Sql.Size = new System.Drawing.Size(917, 155);
            this.Txt_Sql.TabIndex = 21;
            // 
            // Dgv_Sql
            // 
            this.Dgv_Sql.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.Dgv_Sql.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.Dgv_Sql.Location = new System.Drawing.Point(13, 586);
            this.Dgv_Sql.Margin = new System.Windows.Forms.Padding(4);
            this.Dgv_Sql.Name = "Dgv_Sql";
            this.Dgv_Sql.RowHeadersWidth = 51;
            this.Dgv_Sql.Size = new System.Drawing.Size(917, 294);
            this.Dgv_Sql.TabIndex = 22;
            // 
            // Btn_buscar
            // 
            this.Btn_buscar.Font = new System.Drawing.Font("Rockwell", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Btn_buscar.Image = global::Capa_Vista_Componente_Consultas.Properties.Resources.android_search_icon_icons_com_50501;
            this.Btn_buscar.Location = new System.Drawing.Point(613, 10);
            this.Btn_buscar.Margin = new System.Windows.Forms.Padding(4);
            this.Btn_buscar.Name = "Btn_buscar";
            this.Btn_buscar.Size = new System.Drawing.Size(89, 78);
            this.Btn_buscar.TabIndex = 6;
            this.Btn_buscar.Text = "Buscar";
            this.Btn_buscar.TextAlign = System.Drawing.ContentAlignment.TopCenter;
            this.Btn_buscar.TextImageRelation = System.Windows.Forms.TextImageRelation.TextAboveImage;
            this.Btn_buscar.UseCompatibleTextRendering = true;
            this.Btn_buscar.UseVisualStyleBackColor = true;
            this.Btn_buscar.Click += new System.EventHandler(this.Btn_buscar_Click);
            // 
            // Btn_min
            // 
            this.Btn_min.Font = new System.Drawing.Font("Rockwell", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Btn_min.Location = new System.Drawing.Point(1236, 7);
            this.Btn_min.Margin = new System.Windows.Forms.Padding(4);
            this.Btn_min.Name = "Btn_min";
            this.Btn_min.Size = new System.Drawing.Size(112, 30);
            this.Btn_min.TabIndex = 23;
            this.Btn_min.Text = "Minimizar";
            this.Btn_min.UseVisualStyleBackColor = true;
            this.Btn_min.Click += new System.EventHandler(this.Btn_min_Click);
            // 
            // Btn_max
            // 
            this.Btn_max.Font = new System.Drawing.Font("Rockwell", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Btn_max.Location = new System.Drawing.Point(1101, 7);
            this.Btn_max.Margin = new System.Windows.Forms.Padding(4);
            this.Btn_max.Name = "Btn_max";
            this.Btn_max.Size = new System.Drawing.Size(127, 28);
            this.Btn_max.TabIndex = 24;
            this.Btn_max.Text = "Maximizar";
            this.Btn_max.UseVisualStyleBackColor = true;
            this.Btn_max.Click += new System.EventHandler(this.Btn_max_Click);
            // 
            // Frm_Consulta_Compleja
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1442, 893);
            this.ControlBox = false;
            this.Controls.Add(this.Btn_max);
            this.Controls.Add(this.Btn_min);
            this.Controls.Add(this.Dgv_Sql);
            this.Controls.Add(this.Txt_Sql);
            this.Controls.Add(this.Lbl_Sql);
            this.Controls.Add(this.Gpb_Orden);
            this.Controls.Add(this.Lst_Querys);
            this.Controls.Add(this.Lbl_Tabla);
            this.Controls.Add(this.Btn_consimple);
            this.Controls.Add(this.Btn_regresar);
            this.Controls.Add(this.Btn_EjecutarSQL);
            this.Controls.Add(this.Btn_eliminar);
            this.Controls.Add(this.Btn_editar);
            this.Controls.Add(this.Dgv_Tabla);
            this.Controls.Add(this.Btn_buscar);
            this.Controls.Add(this.Lbl_Label1);
            this.Controls.Add(this.Cbo_busqueda);
            this.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Name = "Frm_Consulta_Compleja";
            this.Text = "v";
            ((System.ComponentModel.ISupportInitialize)(this.Dgv_Tabla)).EndInit();
            this.Gpb_Orden.ResumeLayout(false);
            this.Gpb_Orden.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.Dgv_Sql)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.ComboBox Cbo_busqueda;
        private System.Windows.Forms.Label Lbl_Label1;
        private System.Windows.Forms.Button Btn_buscar;
        private System.Windows.Forms.DataGridView Dgv_Tabla;
        private System.Windows.Forms.Button Btn_editar;
        private System.Windows.Forms.Button Btn_eliminar;
        private System.Windows.Forms.Button Btn_EjecutarSQL;
        private System.Windows.Forms.RadioButton Rdb_asc;
        private System.Windows.Forms.RadioButton Rdb_desc;
        private System.Windows.Forms.Button Btn_regresar;
        private System.Windows.Forms.Button Btn_consimple;
        private System.Windows.Forms.Label Lbl_Tabla;
        private System.Windows.Forms.ListBox Lst_Querys;
        private System.Windows.Forms.GroupBox Gpb_Orden;
        private System.Windows.Forms.Label Lbl_Sql;
        private System.Windows.Forms.TextBox Txt_Sql;
        private System.Windows.Forms.DataGridView Dgv_Sql;
        private System.Windows.Forms.Button Btn_min;
        private System.Windows.Forms.Button Btn_max;
    }
}
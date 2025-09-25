
namespace Capa_Vista_Componente_Consultas
{
    partial class Frm_Consultas
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
            this.Gpb_Listado = new System.Windows.Forms.GroupBox();
            this.Gpb_Ordenamiento = new System.Windows.Forms.GroupBox();
            this.Rdb_ASC = new System.Windows.Forms.RadioButton();
            this.Rdb_Opcion1 = new System.Windows.Forms.RadioButton();
            this.Txt_Consulta = new System.Windows.Forms.TextBox();
            this.Lbl_Cadena_Generada = new System.Windows.Forms.Label();
            this.Lbl_Query = new System.Windows.Forms.Label();
            this.Btn_buscar = new System.Windows.Forms.Button();
            this.Cbo_Query = new System.Windows.Forms.ComboBox();
            this.Dgv_Consultas_Simples = new System.Windows.Forms.DataGridView();
            this.Mstp_Consultas = new System.Windows.Forms.MenuStrip();
            this.Btn_Min = new System.Windows.Forms.Button();
            this.Btn_Boton1 = new System.Windows.Forms.Button();
            this.Btn_Boton2 = new System.Windows.Forms.Button();
            this.Btn_Max = new System.Windows.Forms.Button();
            this.Gpb_Listado.SuspendLayout();
            this.Gpb_Ordenamiento.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.Dgv_Consultas_Simples)).BeginInit();
            this.SuspendLayout();
            // 
            // Gpb_Listado
            // 
            this.Gpb_Listado.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.Gpb_Listado.Controls.Add(this.Gpb_Ordenamiento);
            this.Gpb_Listado.Controls.Add(this.Txt_Consulta);
            this.Gpb_Listado.Controls.Add(this.Lbl_Cadena_Generada);
            this.Gpb_Listado.Controls.Add(this.Lbl_Query);
            this.Gpb_Listado.Controls.Add(this.Btn_buscar);
            this.Gpb_Listado.Controls.Add(this.Cbo_Query);
            this.Gpb_Listado.Font = new System.Drawing.Font("Rockwell", 18F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Gpb_Listado.Location = new System.Drawing.Point(0, 32);
            this.Gpb_Listado.Margin = new System.Windows.Forms.Padding(4);
            this.Gpb_Listado.Name = "Gpb_Listado";
            this.Gpb_Listado.Padding = new System.Windows.Forms.Padding(4);
            this.Gpb_Listado.Size = new System.Drawing.Size(1071, 142);
            this.Gpb_Listado.TabIndex = 5;
            this.Gpb_Listado.TabStop = false;
            this.Gpb_Listado.Text = "Consultas Simples";
            // 
            // Gpb_Ordenamiento
            // 
            this.Gpb_Ordenamiento.Controls.Add(this.Rdb_ASC);
            this.Gpb_Ordenamiento.Controls.Add(this.Rdb_Opcion1);
            this.Gpb_Ordenamiento.Font = new System.Drawing.Font("Rockwell", 11F);
            this.Gpb_Ordenamiento.Location = new System.Drawing.Point(603, 43);
            this.Gpb_Ordenamiento.Margin = new System.Windows.Forms.Padding(4);
            this.Gpb_Ordenamiento.Name = "Gpb_Ordenamiento";
            this.Gpb_Ordenamiento.Padding = new System.Windows.Forms.Padding(4);
            this.Gpb_Ordenamiento.Size = new System.Drawing.Size(232, 91);
            this.Gpb_Ordenamiento.TabIndex = 26;
            this.Gpb_Ordenamiento.TabStop = false;
            this.Gpb_Ordenamiento.Text = "Ordenamiento";
            // 
            // Rdb_ASC
            // 
            this.Rdb_ASC.AutoSize = true;
            this.Rdb_ASC.Font = new System.Drawing.Font("Rockwell", 11F);
            this.Rdb_ASC.Location = new System.Drawing.Point(27, 48);
            this.Rdb_ASC.Margin = new System.Windows.Forms.Padding(4);
            this.Rdb_ASC.Name = "Rdb_ASC";
            this.Rdb_ASC.Size = new System.Drawing.Size(68, 25);
            this.Rdb_ASC.TabIndex = 10;
            this.Rdb_ASC.TabStop = true;
            this.Rdb_ASC.Text = "ASC";
            this.Rdb_ASC.UseVisualStyleBackColor = true;
            // 
            // Rdb_Opcion1
            // 
            this.Rdb_Opcion1.AutoSize = true;
            this.Rdb_Opcion1.Font = new System.Drawing.Font("Rockwell", 11F);
            this.Rdb_Opcion1.Location = new System.Drawing.Point(125, 48);
            this.Rdb_Opcion1.Margin = new System.Windows.Forms.Padding(4);
            this.Rdb_Opcion1.Name = "Rdb_Opcion1";
            this.Rdb_Opcion1.Size = new System.Drawing.Size(81, 25);
            this.Rdb_Opcion1.TabIndex = 26;
            this.Rdb_Opcion1.TabStop = true;
            this.Rdb_Opcion1.Text = "DESC";
            this.Rdb_Opcion1.UseVisualStyleBackColor = true;
            // 
            // Txt_Consulta
            // 
            this.Txt_Consulta.BackColor = System.Drawing.SystemColors.ControlLight;
            this.Txt_Consulta.Font = new System.Drawing.Font("Rockwell", 10F);
            this.Txt_Consulta.Location = new System.Drawing.Point(224, 90);
            this.Txt_Consulta.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Txt_Consulta.Name = "Txt_Consulta";
            this.Txt_Consulta.ReadOnly = true;
            this.Txt_Consulta.Size = new System.Drawing.Size(371, 27);
            this.Txt_Consulta.TabIndex = 8;
            // 
            // Lbl_Cadena_Generada
            // 
            this.Lbl_Cadena_Generada.AutoSize = true;
            this.Lbl_Cadena_Generada.Font = new System.Drawing.Font("Rockwell", 11F);
            this.Lbl_Cadena_Generada.Location = new System.Drawing.Point(220, 49);
            this.Lbl_Cadena_Generada.Name = "Lbl_Cadena_Generada";
            this.Lbl_Cadena_Generada.Size = new System.Drawing.Size(204, 21);
            this.Lbl_Cadena_Generada.TabIndex = 7;
            this.Lbl_Cadena_Generada.Text = "Nombre de empleado";
            // 
            // Lbl_Query
            // 
            this.Lbl_Query.AutoSize = true;
            this.Lbl_Query.Font = new System.Drawing.Font("Rockwell", 11F);
            this.Lbl_Query.Location = new System.Drawing.Point(15, 49);
            this.Lbl_Query.Name = "Lbl_Query";
            this.Lbl_Query.Size = new System.Drawing.Size(185, 21);
            this.Lbl_Query.TabIndex = 6;
            this.Lbl_Query.Text = "Nombre de la tabla ";
            // 
            // Btn_buscar
            // 
            this.Btn_buscar.Font = new System.Drawing.Font("Rockwell", 11F);
            this.Btn_buscar.Image = global::Capa_Vista_Componente_Consultas.Properties.Resources.android_search_icon_icons_com_50501;
            this.Btn_buscar.Location = new System.Drawing.Point(843, 45);
            this.Btn_buscar.Margin = new System.Windows.Forms.Padding(4);
            this.Btn_buscar.Name = "Btn_buscar";
            this.Btn_buscar.Size = new System.Drawing.Size(95, 75);
            this.Btn_buscar.TabIndex = 5;
            this.Btn_buscar.Text = "Buscar";
            this.Btn_buscar.TextAlign = System.Drawing.ContentAlignment.TopCenter;
            this.Btn_buscar.TextImageRelation = System.Windows.Forms.TextImageRelation.TextAboveImage;
            this.Btn_buscar.UseCompatibleTextRendering = true;
            this.Btn_buscar.UseVisualStyleBackColor = true;
            // 
            // Cbo_Query
            // 
            this.Cbo_Query.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.Cbo_Query.Font = new System.Drawing.Font("Rockwell", 10F);
            this.Cbo_Query.FormattingEnabled = true;
            this.Cbo_Query.Location = new System.Drawing.Point(16, 87);
            this.Cbo_Query.Margin = new System.Windows.Forms.Padding(4);
            this.Cbo_Query.Name = "Cbo_Query";
            this.Cbo_Query.Size = new System.Drawing.Size(188, 28);
            this.Cbo_Query.TabIndex = 0;
            // 
            // Dgv_Consultas_Simples
            // 
            this.Dgv_Consultas_Simples.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.Dgv_Consultas_Simples.Location = new System.Drawing.Point(16, 182);
            this.Dgv_Consultas_Simples.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Dgv_Consultas_Simples.Name = "Dgv_Consultas_Simples";
            this.Dgv_Consultas_Simples.RowHeadersWidth = 51;
            this.Dgv_Consultas_Simples.RowTemplate.Height = 24;
            this.Dgv_Consultas_Simples.Size = new System.Drawing.Size(1008, 388);
            this.Dgv_Consultas_Simples.TabIndex = 6;
            // 
            // Mstp_Consultas
            // 
            this.Mstp_Consultas.ImageScalingSize = new System.Drawing.Size(20, 20);
            this.Mstp_Consultas.Location = new System.Drawing.Point(0, 0);
            this.Mstp_Consultas.Name = "Mstp_Consultas";
            this.Mstp_Consultas.Padding = new System.Windows.Forms.Padding(5, 2, 0, 2);
            this.Mstp_Consultas.Size = new System.Drawing.Size(1077, 24);
            this.Mstp_Consultas.TabIndex = 9;
            this.Mstp_Consultas.Text = "menuStrip1";
            // 
            // Btn_Min
            // 
            this.Btn_Min.Location = new System.Drawing.Point(976, 1);
            this.Btn_Min.Margin = new System.Windows.Forms.Padding(4);
            this.Btn_Min.Name = "Btn_Min";
            this.Btn_Min.Size = new System.Drawing.Size(85, 28);
            this.Btn_Min.TabIndex = 14;
            this.Btn_Min.Text = "Minimizar";
            this.Btn_Min.UseVisualStyleBackColor = true;
            this.Btn_Min.Click += new System.EventHandler(this.btn_min_Click);
            // 
            // Btn_Boton1
            // 
            this.Btn_Boton1.Location = new System.Drawing.Point(781, 2);
            this.Btn_Boton1.Margin = new System.Windows.Forms.Padding(4);
            this.Btn_Boton1.Name = "Btn_Boton1";
            this.Btn_Boton1.Size = new System.Drawing.Size(95, 28);
            this.Btn_Boton1.TabIndex = 15;
            this.Btn_Boton1.Text = "Regresar";
            this.Btn_Boton1.UseVisualStyleBackColor = true;
            this.Btn_Boton1.Click += new System.EventHandler(this.button1_Click);
            // 
            // Btn_Boton2
            // 
            this.Btn_Boton2.Location = new System.Drawing.Point(588, 2);
            this.Btn_Boton2.Margin = new System.Windows.Forms.Padding(4);
            this.Btn_Boton2.Name = "Btn_Boton2";
            this.Btn_Boton2.Size = new System.Drawing.Size(147, 30);
            this.Btn_Boton2.TabIndex = 16;
            this.Btn_Boton2.Text = "Consulta Compleja";
            this.Btn_Boton2.UseVisualStyleBackColor = true;
            // 
            // Btn_Max
            // 
            this.Btn_Max.Location = new System.Drawing.Point(884, 2);
            this.Btn_Max.Margin = new System.Windows.Forms.Padding(4);
            this.Btn_Max.Name = "Btn_Max";
            this.Btn_Max.Size = new System.Drawing.Size(84, 28);
            this.Btn_Max.TabIndex = 25;
            this.Btn_Max.Text = "Maximizar";
            this.Btn_Max.UseVisualStyleBackColor = true;
            this.Btn_Max.Click += new System.EventHandler(this.btn_max_Click);
            // 
            // Frm_Consultas
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1077, 581);
            this.ControlBox = false;
            this.Controls.Add(this.Gpb_Listado);
            this.Controls.Add(this.Btn_Max);
            this.Controls.Add(this.Btn_Boton2);
            this.Controls.Add(this.Btn_Boton1);
            this.Controls.Add(this.Btn_Min);
            this.Controls.Add(this.Mstp_Consultas);
            this.Controls.Add(this.Dgv_Consultas_Simples);
            this.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.Name = "Frm_Consultas";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Frm_Consultas";
            this.Gpb_Listado.ResumeLayout(false);
            this.Gpb_Listado.PerformLayout();
            this.Gpb_Ordenamiento.ResumeLayout(false);
            this.Gpb_Ordenamiento.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.Dgv_Consultas_Simples)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.GroupBox Gpb_Listado;
        private System.Windows.Forms.Button Btn_buscar;
        private System.Windows.Forms.ComboBox Cbo_Query;
        private System.Windows.Forms.TextBox Txt_Consulta;
        private System.Windows.Forms.Label Lbl_Cadena_Generada;
        private System.Windows.Forms.Label Lbl_Query;
        private System.Windows.Forms.DataGridView Dgv_Consultas_Simples;
        private System.Windows.Forms.MenuStrip Mstp_Consultas;
        private System.Windows.Forms.Button Btn_Min;
        private System.Windows.Forms.Button Btn_Boton1;
        private System.Windows.Forms.Button Btn_Boton2;
        private System.Windows.Forms.Button Btn_Max;
        private System.Windows.Forms.RadioButton Rdb_ASC;
        private System.Windows.Forms.RadioButton Rdb_Opcion1;
        private System.Windows.Forms.GroupBox Gpb_Ordenamiento;
    }
}

namespace Capa_Vista_Componente_Consultas
{
    partial class Frm_Principal
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Frm_Principal));
            this.Btn_ConsultaSimple = new System.Windows.Forms.Button();
            this.Btn_ConsultaCompleja = new System.Windows.Forms.Button();
            this.Btn_Ayuda = new System.Windows.Forms.Button();
            this.Lbl_Bienvenida = new System.Windows.Forms.Label();
            this.Btn_Cerrar = new System.Windows.Forms.Button();
            this.Lbl_Instrucciones = new System.Windows.Forms.Label();
            this.Pic_1 = new System.Windows.Forms.PictureBox();
            ((System.ComponentModel.ISupportInitialize)(this.Pic_1)).BeginInit();
            this.SuspendLayout();
            // 
            // Btn_ConsultaSimple
            // 
            this.Btn_ConsultaSimple.Location = new System.Drawing.Point(440, 137);
            this.Btn_ConsultaSimple.Margin = new System.Windows.Forms.Padding(4);
            this.Btn_ConsultaSimple.Name = "Btn_ConsultaSimple";
            this.Btn_ConsultaSimple.Size = new System.Drawing.Size(155, 66);
            this.Btn_ConsultaSimple.TabIndex = 0;
            this.Btn_ConsultaSimple.Text = "Consultas Simples";
            this.Btn_ConsultaSimple.UseVisualStyleBackColor = true;
            this.Btn_ConsultaSimple.Click += new System.EventHandler(this.btn_ConsultaSimple_Click);
            // 
            // Btn_ConsultaCompleja
            // 
            this.Btn_ConsultaCompleja.Location = new System.Drawing.Point(440, 237);
            this.Btn_ConsultaCompleja.Margin = new System.Windows.Forms.Padding(4);
            this.Btn_ConsultaCompleja.Name = "Btn_ConsultaCompleja";
            this.Btn_ConsultaCompleja.Size = new System.Drawing.Size(155, 69);
            this.Btn_ConsultaCompleja.TabIndex = 1;
            this.Btn_ConsultaCompleja.Text = "Consulta Compleja";
            this.Btn_ConsultaCompleja.UseVisualStyleBackColor = true;
            this.Btn_ConsultaCompleja.Click += new System.EventHandler(this.btn_ConsultaCompleja_Click);
            // 
            // Btn_Ayuda
            // 
            this.Btn_Ayuda.Location = new System.Drawing.Point(440, 337);
            this.Btn_Ayuda.Margin = new System.Windows.Forms.Padding(4);
            this.Btn_Ayuda.Name = "Btn_Ayuda";
            this.Btn_Ayuda.Size = new System.Drawing.Size(155, 62);
            this.Btn_Ayuda.TabIndex = 2;
            this.Btn_Ayuda.Text = "Ayuda";
            this.Btn_Ayuda.UseVisualStyleBackColor = true;
            this.Btn_Ayuda.Click += new System.EventHandler(this.btn_Ayuda_Click);
            // 
            // Lbl_Bienvenida
            // 
            this.Lbl_Bienvenida.AutoSize = true;
            this.Lbl_Bienvenida.Font = new System.Drawing.Font("Microsoft Sans Serif", 15.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lbl_Bienvenida.Location = new System.Drawing.Point(421, 28);
            this.Lbl_Bienvenida.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.Lbl_Bienvenida.Name = "Lbl_Bienvenida";
            this.Lbl_Bienvenida.Size = new System.Drawing.Size(202, 31);
            this.Lbl_Bienvenida.TabIndex = 3;
            this.Lbl_Bienvenida.Text = "BIENVENIDO!";
            // 
            // Btn_Cerrar
            // 
            this.Btn_Cerrar.Location = new System.Drawing.Point(964, 0);
            this.Btn_Cerrar.Margin = new System.Windows.Forms.Padding(4);
            this.Btn_Cerrar.Name = "Btn_Cerrar";
            this.Btn_Cerrar.Size = new System.Drawing.Size(100, 28);
            this.Btn_Cerrar.TabIndex = 4;
            this.Btn_Cerrar.Text = "Cerrar";
            this.Btn_Cerrar.UseVisualStyleBackColor = true;
            this.Btn_Cerrar.Click += new System.EventHandler(this.btn_Cerrar_Click);
            // 
            // Lbl_Instrucciones
            // 
            this.Lbl_Instrucciones.AutoSize = true;
            this.Lbl_Instrucciones.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lbl_Instrucciones.Location = new System.Drawing.Point(329, 80);
            this.Lbl_Instrucciones.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.Lbl_Instrucciones.Name = "Lbl_Instrucciones";
            this.Lbl_Instrucciones.Size = new System.Drawing.Size(377, 25);
            this.Lbl_Instrucciones.TabIndex = 5;
            this.Lbl_Instrucciones.Text = "Por favor, selecciona una de las opciones.";
            // 
            // Pic_1
            // 
            this.Pic_1.Image = ((System.Drawing.Image)(resources.GetObject("Pic_1.Image")));
            this.Pic_1.Location = new System.Drawing.Point(13, 13);
            this.Pic_1.Margin = new System.Windows.Forms.Padding(4);
            this.Pic_1.Name = "Pic_1";
            this.Pic_1.Size = new System.Drawing.Size(105, 107);
            this.Pic_1.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.Pic_1.TabIndex = 6;
            this.Pic_1.TabStop = false;
            // 
            // Frm_Principal
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1067, 554);
            this.ControlBox = false;
            this.Controls.Add(this.Pic_1);
            this.Controls.Add(this.Lbl_Instrucciones);
            this.Controls.Add(this.Btn_Cerrar);
            this.Controls.Add(this.Lbl_Bienvenida);
            this.Controls.Add(this.Btn_Ayuda);
            this.Controls.Add(this.Btn_ConsultaCompleja);
            this.Controls.Add(this.Btn_ConsultaSimple);
            this.Margin = new System.Windows.Forms.Padding(4);
            this.Name = "Frm_Principal";
            this.Text = "Frm_Principal";
            ((System.ComponentModel.ISupportInitialize)(this.Pic_1)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button Btn_ConsultaSimple;
        private System.Windows.Forms.Button Btn_ConsultaCompleja;
        private System.Windows.Forms.Button Btn_Ayuda;
        private System.Windows.Forms.Label Lbl_Bienvenida;
        private System.Windows.Forms.Button Btn_Cerrar;
        private System.Windows.Forms.Label Lbl_Instrucciones;
        private System.Windows.Forms.PictureBox Pic_1;
    }
}
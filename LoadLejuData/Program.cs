using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.Threading;
using MySql.Data.MySqlClient;
using System.Configuration;
using System.IO;

namespace LoadLejuData
{
    class Program
    {
        static void Main(string[] args)
        {



            string connectionString = ConfigurationManager.ConnectionStrings["DestinationDatabase"].ConnectionString;

            //string sqlquery = "select id from error";

            //DataSet ds = new DataSet();

            //ds = Query(connectionString, sqlquery);

            string connMysql = ConfigurationManager.ConnectionStrings["SourceDatabase"].ConnectionString;

            MySqlConnection conn = new MySqlConnection(connMysql);

            int whichday = Convert.ToInt32(ConfigurationManager.AppSettings["whichday"]);

            string day = DateTime.Now.AddDays(-1 * whichday).ToString("yyyyMMdd");
       

            try
            {
                
                DateTime begin = DateTime.Now;

                conn.Open();

                DataTable mysqlData = new DataTable();

                string mysqlstring = string.Format("SELECT id,city_en,hid,day,pv,uv,source FROM m_gatherweb_house where day='{0}';",day);

                MySqlDataAdapter da = new MySqlDataAdapter(mysqlstring, conn);
                MySqlCommandBuilder cb = new MySqlCommandBuilder(da);

                da.Fill(mysqlData);

                DataTable mysqlData_pc = new DataTable();

                string mysqlstring_pc = string.Format(@"SELECT id
      ,hid
      ,pv
      ,uv
      ,city_en
      ,date FROM m_gatherweb_pc_house where date='{0}';", day);

                MySqlDataAdapter da_pc = new MySqlDataAdapter(mysqlstring_pc, conn);
                MySqlCommandBuilder cb_pc = new MySqlCommandBuilder(da_pc);

                da_pc.Fill(mysqlData_pc);

                conn.Close();


                //string sql = string.Format(@"insert into [dbo].[m_gatherweb_house] with(rowlock)
                //                            select * from openquery (LEJULAIFANG,'SELECT id,city_en,hid,day,pv,uv,source FROM m_data_leju_com.m_gatherweb_house where id>=28943165+" + (Convert.ToInt32(ds.Tables[0].Rows[i][0]) * 100000).ToString() + " and id < 28943165+" + ((Convert.ToInt32(ds.Tables[0].Rows[i][0]) + 1) * 100000).ToString() + ";')");


                SqlBCP(connectionString, mysqlData_pc, "m_gatherweb_pc_house");
                //Excsql(connectionString, sql);
                SqlBCP(connectionString, mysqlData, "m_gatherweb_house");

                DateTime end = DateTime.Now;

                TimeSpan ts = begin - end;

                WriteLog(string.Format("{0}成功--PC。插入时间{1}ms。插入数据量{2}条", day, ts.TotalMilliseconds, mysqlData_pc.Rows.Count.ToString()));
                WriteLog(string.Format("{0}成功。插入时间{1}ms。插入数据量{2}条", day, ts.TotalMilliseconds, mysqlData.Rows.Count.ToString()));

                Console.WriteLine(string.Format("{0}成功--PC。插入时间{1}ms。插入数据量{2}条", day, ts.TotalMilliseconds, mysqlData_pc.Rows.Count.ToString()));
                Console.WriteLine(string.Format("{0}成功。插入时间{1}ms。插入数据量{2}条", day,ts.TotalMilliseconds,mysqlData.Rows.Count.ToString()));

                Thread.Sleep(1000);
                Console.WriteLine("2s");
                Thread.Sleep(1000);
                Console.WriteLine("1s");

                Thread.Sleep(1000);
                Console.WriteLine("0s");

                //string sqldelete = string.Format("delete error where id = "+ ds.Tables[0].Rows[i][0].ToString());

                //Excsql(connectionString, sqldelete);



            }
            catch (Exception e)
            {
                //string sql = string.Format("insert into Error(id) values ({0})",i.ToString());

                //Excsql(connectionString, sql);
                WriteLog(e.Message);
                Console.WriteLine(e.Message);
            }



        }

        public static void Excsql(string ConnString, string sql)
        {

            using (System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection())
            {
                conn.ConnectionString = ConnString;
                conn.Open();
                using (System.Data.SqlClient.SqlCommand command = conn.CreateCommand())
                {
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }
                conn.Close();
            }

        }

        public static DataSet Query(string ConnString, string sql)
        {
            System.Data.DataSet result = new System.Data.DataSet();
            using (System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection())
            {
                conn.ConnectionString = ConnString;
                conn.Open();
                using (System.Data.SqlClient.SqlCommand command = conn.CreateCommand())
                {
                    command.CommandText = sql;
                    using (System.Data.SqlClient.SqlDataAdapter adp = new System.Data.SqlClient.SqlDataAdapter(command))
                    {
                        adp.Fill(result);
                    }
                }
                conn.Close();
            }
            return result;
        }

        public static void SqlBCP(string ConnString, DataTable dt, string destablename)
        {

            SqlBulkCopy sqlBulkCopyMobile = new SqlBulkCopy(ConnString);
            sqlBulkCopyMobile.DestinationTableName = destablename;
            sqlBulkCopyMobile.WriteToServer(dt);
            sqlBulkCopyMobile.Close();


        }

        public static void WriteLog(string log)
        {

            //string str = System.Environment.CurrentDirectory;
            if (File.Exists(Environment.CurrentDirectory + @"/log/log-" + DateTime.Today.ToShortDateString().Replace("/", "-") + ".txt"))
            {
                string strFilePath = Environment.CurrentDirectory + @"/log/log-" + DateTime.Today.ToShortDateString().Replace("/", "-") + ".txt";
                FileStream fs = new FileStream(strFilePath, FileMode.Append);
                StreamWriter sw = new StreamWriter(fs, Encoding.Default);
                sw.WriteLine(DateTime.Now.ToString() + "|" + log);
                sw.Close();
                fs.Close();
            }
            else
            {
                string strFilePath = Environment.CurrentDirectory + @"/log/log-" + DateTime.Today.ToShortDateString().Replace("/", "-") + ".txt";
                File.Create(strFilePath).Dispose();
                FileStream fs = new FileStream(strFilePath, FileMode.Append);
                StreamWriter sw = new StreamWriter(fs, Encoding.Default);
                sw.WriteLine(DateTime.Now.ToString() + "|" + log);
                sw.Close();
                fs.Close();
            }
        }


    }
}

using System;
using System.Data.SqlClient;
using System.Xml;
using System.Windows.Forms;

namespace FarmerPostTo300.src
{
    class Connection
    {
        public SqlConnection conn = null;
        public static string SqlCon;
        public SqlConnection CompanyDbConnect()
        {
            string SettingsPath = Application.StartupPath + "\\connectionValidate.xml";
            XmlDocument settings = new XmlDocument();
            settings.Load(SettingsPath);
            // Select a specific node
            XmlNode node = settings.SelectSingleNode("/Settings/SQLconnectionString/Value");
            // Get its value
            SqlCon = node.InnerText;
            Console.WriteLine(SqlCon);
            conn = new SqlConnection(SqlCon);
            return conn;
        }

        public SqlConnection CompanyDbConnectSage()
        {
            string SettingsPath = Application.StartupPath + "\\connectionValidateSage.xml";
            XmlDocument settings = new XmlDocument();
            settings.Load(SettingsPath);
            // Select a specific node
            XmlNode node = settings.SelectSingleNode("/Settings/SQLconnectionString/Value");
            // Get its value
            SqlCon = node.InnerText;
            Console.WriteLine(SqlCon);
            conn = new SqlConnection(SqlCon);
            return conn;
        }

    }
}

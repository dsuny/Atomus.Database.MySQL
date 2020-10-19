using System;
using System.Data.Common;
using MySql.Data.MySqlClient;

namespace Atomus.Database
{
    public class MySQL : IDatabase
    {
        MySqlDataAdapter sqlDataAdapter;

        public MySQL()
        {
            //string path;
            //System.IO.FileStream fileStream;
            //string fileName;

            //try
            //{
            //    path = System.Environment.CurrentDirectory;
            //    //path = path.Substring(0, path.LastIndexOf("\\") + 1);
            //}
            //catch (AtomusException exception)
            //{
            //    throw exception;
            //}
            //catch (Exception exception)
            //{
            //    throw new AtomusException(exception);
            //}

            //fileStream = null;
            //try
            //{
            //    fileName = string.Format("{0}\\{1}", path,  "MySql.Data.dll");

            //    if (System.IO.File.Exists(fileName))
            //        System.IO.File.Delete(fileName);

            //    fileStream = System.IO.File.Create(fileName);
            //    fileStream.Write(Properties.Resources.MySql_Data, 0, Properties.Resources.MySql_Data.Length);
            //}
            //catch (AtomusException exception)
            //{
            //    throw exception;
            //}
            //catch (Exception exception)
            //{
            //    throw new AtomusException(exception);
            //}
            //finally
            //{
            //    fileStream?.Close();
            //}


            this.sqlDataAdapter = new MySqlDataAdapter
            {
                SelectCommand = new MySqlCommand
                {
                    Connection = new MySqlConnection()
                }
            };
            //this.sqlDataAdapter.SelectCommand.Connection = new SqlConnection();
        }

        DbParameter IDatabase.AddParameter(string parameterName, DbType dbType, int size)
        {
            MySqlCommand sqlCommand;

            try
            {
                sqlCommand = this.sqlDataAdapter.SelectCommand;

                if (size == 0)
                    return sqlCommand.Parameters.Add(parameterName, this.DbTypeConvert(dbType));
                else
                    return sqlCommand.Parameters.Add(parameterName, this.DbTypeConvert(dbType), size);
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }

        MySqlDbType DbTypeConvert(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.BigInt:
                    return MySqlDbType.Int64;

                case DbType.Binary:
                    return MySqlDbType.Binary;

                case DbType.Bit:
                    return MySqlDbType.Bit;

                case DbType.Char:
                    return MySqlDbType.String;

                case DbType.Date:
                    return MySqlDbType.Date;

                case DbType.DateTime:
                    return MySqlDbType.DateTime;

                case DbType.DateTime2:
                    return MySqlDbType.DateTime;

                case DbType.DateTimeOffset:
                    return MySqlDbType.Timestamp;

                case DbType.Decimal:
                    return MySqlDbType.Decimal;

                case DbType.Float:
                    return MySqlDbType.Float;

                case DbType.Image:
                    return MySqlDbType.LongBlob;

                case DbType.Int:
                    return MySqlDbType.Int32;

                case DbType.Money:
                    return MySqlDbType.Int32;

                case DbType.NChar:
                    return MySqlDbType.String;

                case DbType.NText:
                    return MySqlDbType.Text;

                case DbType.NVarChar:
                    return MySqlDbType.VarChar;

                case DbType.Real:
                    return MySqlDbType.Float;

                case DbType.SmallDateTime:
                    throw new AtomusException("DbType.Structured type Not Support.");

                case DbType.SmallInt:
                    return MySqlDbType.Int16;

                case DbType.SmallMoney:
                    return MySqlDbType.Int16;

                case DbType.Structured:
                    throw new AtomusException("DbType.Structured type Not Support.");

                case DbType.Text:
                    return MySqlDbType.Text;

                case DbType.Time:
                    return MySqlDbType.Time;

                case DbType.Timestamp:
                    return MySqlDbType.Timestamp;

                case DbType.TinyInt:
                    return MySqlDbType.UByte;

                case DbType.Udt:
                    throw new AtomusException("DbType.Udt type Not Support.");

                case DbType.UniqueIdentifier:
                    return MySqlDbType.Guid;

                case DbType.VarBinary:
                    return MySqlDbType.VarBinary;

                case DbType.VarChar:
                    return MySqlDbType.VarChar;

                case DbType.Variant:
                    throw new AtomusException("DbType.Variant type Not Support.");

                case DbType.Xml:
                    throw new AtomusException("DbType.Xml type Not Support.");

                default:
                    throw new AtomusException("default type Not Support.");
            }
        }

        DbCommand IDatabase.Command
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand;
            }
        }

        DbConnection IDatabase.Connection
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand.Connection;
            }
        }

        DbDataAdapter IDatabase.DataAdapter
        {
            get
            {
                return this.sqlDataAdapter;
            }
        }

        DbTransaction IDatabase.Transaction
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand.Transaction;
            }
        }

        void IDatabase.DeriveParameters()
        {
            try
            {
                MySqlCommandBuilder.DeriveParameters(this.sqlDataAdapter.SelectCommand);
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }

        void IDatabase.Close()
        {
            try
            {
                if (this.sqlDataAdapter.SelectCommand.Connection != null)
                {
                    this.sqlDataAdapter.SelectCommand.Connection.Close();
                    this.sqlDataAdapter.SelectCommand.Connection.Dispose();
                }

                if (this.sqlDataAdapter.SelectCommand != null)
                {
                    this.sqlDataAdapter.SelectCommand.Dispose();
                }

                if (this.sqlDataAdapter != null)
                {
                    this.sqlDataAdapter.Dispose();
                }
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }
    }
}
using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit;

namespace SideBySide
{
	public class Transaction : IClassFixture<TransactionFixture>
	{
		public Transaction(TransactionFixture database)
		{
			m_database = database;
			m_connection = m_database.Connection;
			m_connection.Execute("delete from transactions_test");
		}

		[Theory]
		[InlineData(IsolationLevel.ReadUncommitted, "read uncommitted")]
		[InlineData(IsolationLevel.ReadCommitted, "read committed")]
		[InlineData(IsolationLevel.RepeatableRead, "repeatable read")]
		[InlineData(IsolationLevel.Serializable, "serializable")]
		[InlineData(IsolationLevel.Unspecified, "repeatable read")]
#if !NET452 && !NET461 && !NET472
		[InlineData(IsolationLevel.Snapshot, "repeatable read")]
#endif
		public void DbConnectionIsolationLevel(IsolationLevel inputIsolationLevel, string expectedTransactionIsolationLevel)
		{
			DbConnection connection = m_connection;
			m_connection.Execute(@"set global log_output = 'table';");
			m_connection.Execute(@"set global general_log = 1;");
			using (var trans = connection.BeginTransaction(inputIsolationLevel))
			{
				trans.Commit();
			}

			m_connection.Execute(@"set global general_log = 0;");
			var results = connection.Query<string>($"select convert(argument USING utf8) from mysql.general_log where thread_id = {m_connection.ServerThread} order by event_time desc limit 10;");
			var lastIsolationLevelQuery = results.First(x => x.ToLower().Contains("isolation"));

			if (IsMySqlVersionLessThan57(m_connection.ServerVersion))
			{
				Assert.Contains("serializable", lastIsolationLevelQuery.ToLower());
				return;
			}

			Assert.Contains(expectedTransactionIsolationLevel.ToLower(), lastIsolationLevelQuery.ToLower());
		}

		[Theory]
		[InlineData(IsolationLevel.ReadUncommitted, "start transaction")]
		[InlineData(IsolationLevel.ReadCommitted, "start transaction")]
		[InlineData(IsolationLevel.RepeatableRead, "start transaction")]
		[InlineData(IsolationLevel.Serializable, "start transaction")]
		[InlineData(IsolationLevel.Unspecified, "start transaction")]
#if !NET452 && !NET461 && !NET472
		[InlineData(IsolationLevel.Snapshot, "start transaction with consistent snapshot")]
#endif
		public void DbConnectionTransactionCommand(IsolationLevel inputIsolationLevel, string expectedTransactionIsolationLevel)
		{
			DbConnection connection = m_connection;
			m_connection.Execute(@"set global log_output = 'table';");
			m_connection.Execute(@"set global general_log = 1;");
			using (var trans = connection.BeginTransaction(inputIsolationLevel))
			{
				trans.Commit();
			}

			m_connection.Execute(@"set global general_log = 0;");
			var results = connection.Query<string>($"select convert(argument USING utf8) from mysql.general_log where thread_id = {m_connection.ServerThread} order by event_time desc limit 10;");
			var lastStartTransactionQuery = results.First(x => x.ToLower().Contains("start"));

			if (IsMySqlVersionLessThan57(m_connection.ServerVersion))
			{
				Assert.Contains("start transaction", lastStartTransactionQuery.ToLower());
				return;
			}

			Assert.Contains(expectedTransactionIsolationLevel.ToLower(), lastStartTransactionQuery.ToLower());
		}

		private bool IsMySqlVersionLessThan57(string currentVersionStr)
		{
			var version = new Version("5.7");
			var currentVersion = new Version(currentVersionStr);
			var result = version.CompareTo(currentVersion);
			return result > 0;
		}


		readonly TransactionFixture m_database;
		readonly MySqlConnection m_connection;
	}
}

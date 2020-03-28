using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;
using Xunit;
using Xunit.Abstractions;

namespace SideBySide
{
	public class Transaction : IClassFixture<TransactionFixture>
	{
		private readonly ITestOutputHelper _output;

		public Transaction(TransactionFixture database, ITestOutputHelper output)
		{
			m_database = database;
			m_connection = m_database.Connection;
			m_connection.Execute("delete from transactions_test");
			_output = output;
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

			if (IsMySqlAndVersionLessThan57(m_connection.ServerVersion))
			{
				Assert.Contains("serializable", lastIsolationLevelQuery.ToLower());
				return;
			}

			Assert.Contains(expectedTransactionIsolationLevel.ToLower(), lastIsolationLevelQuery.ToLower());
		}

#if !NET452 && !NET461 && !NET472
		[Theory]
		[InlineData(IsolationLevel.ReadUncommitted, "start transaction")]
		[InlineData(IsolationLevel.ReadCommitted, "start transaction")]
		[InlineData(IsolationLevel.RepeatableRead, "start transaction")]
		[InlineData(IsolationLevel.Serializable, "start transaction")]
		[InlineData(IsolationLevel.Unspecified, "start transaction")]
		[InlineData(IsolationLevel.Snapshot, "start transaction with consistent snapshot")]
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

			for (int x = 0; x > results.Count(); x++)
			{
				Console.WriteLine($"results: {results.ElementAt(x)}");
				_output.WriteLine($"results: {results.ElementAt(x)}");
			}

			Assert.Equal("Version", m_connection.ServerVersion);
			Assert.Equal(results.ElementAt(2), results.ElementAt(3));
			var lastStartTransactionQuery = results.First(x => x.ToLower().Contains("start"));

			if (IsMySqlAndVersionLessThan57(m_connection.ServerVersion))
			{
				Assert.Contains("start transaction", lastStartTransactionQuery.ToLower());
				return;
			}

			Assert.Contains(expectedTransactionIsolationLevel.ToLower(), lastStartTransactionQuery.ToLower());
		}
#endif

		private bool IsMySqlAndVersionLessThan57(string currentVersionStr)
		{
			var version = new Version("5.7");
			Version currentVersion = null;

			if (Version.TryParse(currentVersionStr, out currentVersion))
			{
				var result = version.CompareTo(currentVersion);
				return result > 0;
			}

			return false;
		}

		readonly TransactionFixture m_database;
		readonly MySqlConnection m_connection;
	}
}

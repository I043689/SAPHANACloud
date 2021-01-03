import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DeployToHANA {
	private static final String HANA_USER = "YIKAI_DEMO_HDI_DB_1_EXQFN5M71M42S3VGKFVNANV8M_DT";
	private static final String HANA_PASSWORD = "Jt47h1hm-7tUXny-Llu2wp18Df9EfSJHBa2C-lcQTtwS85.1aAbQ0VMuh.F1CWhiqRB2rDFwDN46zkwpKF5pjm_V7Elul3B4.2KUSf4R4phmLl6JacNBSvIpMIrpmixo";
	private static final String HANA_URL = "jdbc:sap://60be6da0-6657-4d9e-8856-714068d0aafd.hana.canary.cn-shanghai.antero.dbaas.ondemand.com:30015?encrypt=true&validateCertificate=true";

	private static final String CAL_CULATION_VIEW_NAME = "src/models/PERFORMANCE_SALARIES_BY_JAVA";
	private static final String CONTAINER_NAME = "YIKAI_DEMO_HDI_DB_1";

	private static final String SQL_PRE_1 = "CREATE LOCAL TEMPORARY COLUMN TABLE #FILESFOLDERS_WRITE LIKE _SYS_DI.TT_FILESFOLDERS_CONTENT";
	private static final String SQL_PRE_2 = "CREATE LOCAL TEMPORARY COLUMN TABLE #FILESFOLDERS_PARAMETERS LIKE _SYS_DI.TT_FILESFOLDERS_PARAMETERS";
	private static final String SQL_PRE_3 = "CREATE LOCAL TEMPORARY COLUMN TABLE #FILESFOLDERS LIKE _SYS_DI.TT_FILESFOLDERS";
	private static final String SQL_PRE_4 = "CREATE LOCAL TEMPORARY COLUMN TABLE #FILESFOLDERS_MAKE LIKE _SYS_DI.TT_FILESFOLDERS";

	private static final String SQL_EXE_1 = "INSERT INTO #FILESFOLDERS_WRITE (PATH, CONTENT) VALUES ('"
			+ CAL_CULATION_VIEW_NAME + ".hdbcalculationview','" + "<xml>')";
	private static final String SQL_EXE_2 = "CALL " + CONTAINER_NAME
			+ "#DI.WRITE(#FILESFOLDERS_WRITE, _SYS_DI.T_NO_PARAMETERS, ?, ?, ?)";
	private static final String SQL_EXE_3 = "INSERT INTO #FILESFOLDERS_MAKE (PATH) VALUES ('" + CAL_CULATION_VIEW_NAME
			+ ".hdbcalculationview')";
	private static final String SQL_EXE_4 = "CALL " + CONTAINER_NAME
			+ "#DI.MAKE(#FILESFOLDERS_MAKE, #FILESFOLDERS, #FILESFOLDERS_PARAMETERS, _SYS_DI.T_NO_PARAMETERS, ?, ?, ?)";

	private static final String SQL_POST_1 = "DROP TABLE #FILESFOLDERS_WRITE";
	private static final String SQL_POST_2 = "DROP TABLE #FILESFOLDERS_PARAMETERS";
	private static final String SQL_POST_3 = "DROP TABLE #FILESFOLDERS";
	private static final String SQL_POST_4 = "DROP TABLE #FILESFOLDERS_MAKE";

	public static void main(String[] args) {

		Connection connection = null;
		try {
			connection = DriverManager.getConnection(HANA_URL, HANA_USER, HANA_PASSWORD);
		} catch (SQLException e) {
			System.err.println("Connection Failed:");
			System.err.println(e);
		}
		if (connection != null) {
			try {
				System.out.println("Connection to HANA successful!");
				Statement stmt_pre_1 = connection.createStatement();
				stmt_pre_1.execute(SQL_PRE_1);
				System.out.println(SQL_PRE_1);
				
				Statement stmt_pre_2 = connection.createStatement();
				stmt_pre_2.execute(SQL_PRE_2);
				System.out.println(SQL_PRE_2);
				
				Statement stmt_pre_3 = connection.createStatement();
				stmt_pre_3.execute(SQL_PRE_3);
				System.out.println(SQL_PRE_3);
				
				Statement stmt_pre_4 = connection.createStatement();
				stmt_pre_4.execute(SQL_PRE_4);
				System.out.println(SQL_PRE_4);

				String xml = CreateCaculationView.createRoot().toXMLWithXSDValidation();
				String SQL_EXE_1_WITH_XML = SQL_EXE_1.replace("<xml>", xml);
				
				Statement stmt_exe_1 = connection.createStatement();
				stmt_exe_1.execute(SQL_EXE_1_WITH_XML);
				System.out.println(SQL_EXE_1_WITH_XML);
				
				Statement stmt_exe_2 = connection.createStatement();
				stmt_exe_2.execute(SQL_EXE_2);
				System.out.println(SQL_EXE_2);
				
				Statement stmt_exe_3 = connection.createStatement();
				stmt_exe_3.execute(SQL_EXE_3);
				System.out.println(SQL_EXE_3);
				
				Statement stmt_exe_4 = connection.createStatement();
				stmt_exe_4.execute(SQL_EXE_4);
				System.out.println(SQL_EXE_4);

				Statement stmt_post_1 = connection.createStatement();
				stmt_post_1.execute(SQL_POST_1);
				System.out.println(SQL_POST_1);
				
				Statement stmt_post_2 = connection.createStatement();
				stmt_post_2.execute(SQL_POST_2);
				System.out.println(SQL_POST_2);
				
				Statement stmt_post_3 = connection.createStatement();
				stmt_post_3.execute(SQL_POST_3);
				System.out.println(SQL_POST_3);
				
				Statement stmt_post_4 = connection.createStatement();
				stmt_post_4.execute(SQL_POST_4);
				System.out.println(SQL_POST_4);

				connection.close();
			} catch (SQLException e) {
				System.err.println(e);
				System.err.println("Execution failed!");
			}
		}
	}

}

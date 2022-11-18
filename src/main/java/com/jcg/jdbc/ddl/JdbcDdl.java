package com.jcg.jdbc.ddl;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class JdbcDdl{
	// Nombre JDBC Driver y URL base de datos
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";  
    static final String JDBC_DB_URL = "jdbc:mysql://localhost:3306";
 
    // Credenciales base de datos JDBC
    static final String JDBC_USER = "root";
    static final String JDBC_PASS = "mysqlrootpasS1-";
 
    public final static Logger logger = Logger.getLogger(JdbcDdl.class);
    
	private static final String DATABASE_NAME = "tutorialJdbcDdl";
	private static final String TABLE_NAME = "employee";
 
    public static void main(String[] args) {
    	BasicConfigurator.configure();	// para log4j
        Connection connObj = null;
        Statement stmtOBj = null;
        try {
            Class.forName(JDBC_DRIVER);
            connObj = DriverManager.getConnection(JDBC_DB_URL, JDBC_USER, JDBC_PASS);
 
            stmtOBj = connObj.createStatement();
 
            // DDL Statement 1 - Creación de la base de datos
            logger.info("\n=======CREATE " + DATABASE_NAME + " DATABASE=======");           
            stmtOBj.executeUpdate("CREATE DATABASE " + DATABASE_NAME + ";");
            logger.info("\n=======DATABASE IS SUCCESSFULLY CREATED=======\n");
 
            logger.info("\n=======USING " + DATABASE_NAME + " DATABASE=======\n");
            stmtOBj.executeUpdate("USE " + DATABASE_NAME + ";");
 
            // DDL Statement 2 - Crear una tabla
            logger.info("\n=======CREATE " + TABLE_NAME + " TABLE=======");         
            stmtOBj.executeUpdate("CREATE TABLE " + TABLE_NAME + 
            						" (emp_id int, name varchar(50));");
            logger.info("\n=======TABLE IS SUCCESSFULLY CREATED=======\n");
 
            logger.info("\n=======SHOW TABLE STRUCTURE=======");
            showDbTableStructure();
 
            // DDL Statement 3 - Modificar columna de tabla añadiendo edad
            logger.info("\n=======ALTER " + TABLE_NAME + " TABLE=======");
            stmtOBj.executeUpdate("ALTER TABLE " + TABLE_NAME + " ADD age int");
            logger.info("\n=======TABLE IS SUCCESSFULLY ALTERED=======\n");
 
            logger.info("\n=======SHOW TABLE STRUCTURE=======");
            showDbTableStructure();     
 
            // DDL Statement 4(a) - Eliminar columna de la tabla
            logger.info("\n=======DROP COLUMN=======");
            stmtOBj.executeUpdate("ALTER TABLE " + TABLE_NAME + " DROP COLUMN age");
            logger.info("\n=======COLUMN IS SUCCESSFULLY DROPPED FROM THE TABLE=======\n");
 
            logger.info("\n=======SHOW TABLE STRUCTURE=======");
            showDbTableStructure(); 
 
            // DDL Statement 4(b) - Eliminar tabla
            logger.info("\n=======DROP TABLE=======");
            stmtOBj.executeUpdate("DROP TABLE " + TABLE_NAME + ";" );
            logger.info("\n=======TABLE IS SUCCESSFULLY DROPPED FROM THE DATABASE=======\n");
 
            // DDL Statement 4(c) - Eliminar base de datos
            logger.info("\n=======DROP DATABASE=======");
            stmtOBj.executeUpdate("DROP DATABASE " + DATABASE_NAME + ";");
            logger.info("\n=======DATABASE IS SUCCESSFULLY DROPPED=======");
        } catch(Exception sqlException) {
            sqlException.printStackTrace();
        } finally {
            try {
                if(stmtOBj != null) {
                    stmtOBj.close();    // Cerrar objeto de declaración
                }
                if(connObj != null) {
                    connObj.close();    // Cerrar objeto de conexión
                }
            } catch (Exception sqlException) {
                sqlException.printStackTrace();
            }
        }
    }
 
    // Imprimir estructura de la tabla
    private static void showDbTableStructure() throws SQLException {
        StringBuilder builderObj = new StringBuilder();
        DatabaseMetaData metaObj = DriverManager.getConnection(JDBC_DB_URL, JDBC_USER, JDBC_PASS).getMetaData();
        ResultSet resultSetObj = metaObj.getColumns(DATABASE_NAME, null, TABLE_NAME, "%");
 
        builderObj.append(TABLE_NAME + " Columns Are?= (");
        while (resultSetObj.next()) {
            String columnName = resultSetObj.getString(4);
            builderObj.append(columnName).append(", ");
        }
        builderObj.deleteCharAt(builderObj.lastIndexOf(",")).deleteCharAt(builderObj.lastIndexOf(" ")).append(")").append("\n");
        logger.info(builderObj.toString());
    }
}

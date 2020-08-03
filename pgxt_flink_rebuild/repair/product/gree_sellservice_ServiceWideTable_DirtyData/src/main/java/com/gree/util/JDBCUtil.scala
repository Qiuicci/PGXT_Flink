package com.gree.util

import java.sql.PreparedStatement
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import org.slf4j.{Logger, LoggerFactory}


object JDBCUtil {

    val dataSource = initConnection()
//    val dataSource1 = initConnection1()



     // 初始化 连接TIDB-dwd_sellinfor_n01库的连接
    def initConnection()={
      val properties = new Properties()
      //获取配置文件中的mysql数据库连接
      val config = ConfigurationUtil("config.properties")
      properties.setProperty("driverClassName","com.mysql.jdbc.Driver")
      properties.setProperty("url",config.getString("jdbc.url"))
      properties.setProperty("username",config.getString("jdbc.user"))
      properties.setProperty("password",config.getString("jdbc.password"))
      properties.setProperty("maxActive",config.getString("jdbc.maxActive"))
      properties.setProperty("initialSize",config.getString("jdbc.initialSize"))
      properties.setProperty("minIdle",config.getString("jdbc.minIdle"))
        properties.setProperty("maxWait",config.getString("jdbc.maxWait"))
      //获取连接池对象
      DruidDataSourceFactory.createDataSource(properties)

    }
    //初始化 连接TIDB-app_greedatatable_nw库的连接
//    def initConnection1()={
//      val properties = new Properties()
//      //获取配置文件中的mysql数据库连接
//      val config = ConfigurationUtil("config.properties")
//      properties.setProperty("driverClassName","com.mysql.jdbc.Driver")
//      properties.setProperty("url",config.getString("jdbc.url1"))
//      properties.setProperty("username",config.getString("jdbc.user1"))
//      properties.setProperty("password",config.getString("jdbc.password1"))
//      properties.setProperty("maxActive",config.getString("jdbc.maxActive"))
//      properties.setProperty("initialSize",config.getString("jdbc.initialSize"))
//      properties.setProperty("minIdle",config.getString("jdbc.minIdle"))
//      properties.setProperty("maxWait",config.getString("jdbc.maxWait"))
//      //获取连接池对象
//      DruidDataSourceFactory.createDataSource(properties)
//
//    }


     // 执行单条sql语句：insert into table values(?,?,?)

    def executeUpdate(sql:String,args:Array[Any]): Unit ={
      val logger: Logger = LoggerFactory.getLogger(JDBCUtil.getClass)
      val conn = dataSource.getConnection
     var ps:PreparedStatement = null
      try {
        conn.setAutoCommit(false)
        ps = conn.prepareStatement(sql)
        if (ps!=null){
        if (args != null && args.length > 0) {
          (0 until args.length).foreach {
            i => ps.setObject(i + 1, args(i))
          }
        }
        ps.executeUpdate()
        conn.commit()
        }
      } catch {
        case e:Exception =>{
          conn.rollback()
          logger.error("gree_new_repairboard数据库插入或删除异常"+e.getMessage)
        }
      } finally {
        ps.close()
        conn.close()
      }
    }

    // 批处理sql插入sql:insert into table value(?,?,?)

    def executeBatchUpdate(sql:String,argsList:Iterable[Array[Any]]): Unit ={
      val conn = dataSource.getConnection
      conn.setAutoCommit(false)
      val ps = conn.prepareStatement(sql)
      argsList.foreach{
        case args : Array[Any] =>{
          (0 until args.length).foreach{
            i => ps.setObject(i+1,args(i))
          }
          ps.addBatch()
        }
      }
      ps.executeBatch()
      conn.commit()
    }

  // 执行单条sql语句：insert into table values(?,?,?)
//  def executeUpdate1(sql:String,args:Array[Any]): Unit ={
//    val logger: Logger = LoggerFactory.getLogger(JDBCUtil.getClass)
//    val conn = dataSource1.getConnection
//    var ps:PreparedStatement = null
//    try {
//      conn.setAutoCommit(false)
//      ps = conn.prepareStatement(sql)
//      if (ps!=null){
//        if (args != null && args.length > 0) {
//          (0 until args.length).foreach {
//            i => ps.setObject(i + 1, args(i))
//          }
//        }
//        ps.executeUpdate()
//        conn.commit()
//      }
//    } catch {
//      case e:Exception =>{
//        conn.rollback()
//        logger.error("gree_new_repairboard数据库插入或删除异常"+e.getMessage)
//      }
//    } finally {
//      ps.close()
//      conn.close()
//    }
//  }
  }





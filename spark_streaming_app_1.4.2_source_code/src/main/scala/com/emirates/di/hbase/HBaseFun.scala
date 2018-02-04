package com.emirates.di.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{RegexStringComparator, RowFilter, SubstringComparator}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

import scala.collection.JavaConversions._
import scala.collection.mutable
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.security.UserGroupInformation
import java.io.IOException
import java.nio.file.Path
import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration


/**
  * Created by Vijaya.Ramisetti on 28/01/2018.
  */
object HBaseFun extends Serializable{

  val conf: Configuration = HBaseConfiguration.create()

  val connection: Connection = createConnectionLocal
  val hBaseAdmin: Admin = createAdmin
  val hTableMap: mutable.Map[String, Table] = mutable.Map.empty[String, Table]


  def createConnectionLocal: Connection = {
    var localConnection: Connection = null

    if (!UserGroupInformation.isSecurityEnabled) throw new IOException("Security is not enabled in core-site.xml")
    try {
      UserGroupInformation.setConfiguration(conf)
      val userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hbase-ukda_cluster_onprem@UKDS.AC.UK", "hbase.headless.keytab")
      UserGroupInformation.setLoginUser(userGroupInformation)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }

    localConnection = ConnectionFactory.createConnection(conf)

    localConnection
  }

  def createAdmin: Admin = {
    var hAdmin: Admin = null
    try{
      hAdmin = connection.getAdmin
      sys.addShutdownHook(
        hAdmin.close()
      )
      println("HBase admin is created")
    }catch{
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    hAdmin
  }

  def isTablePresent(tableName: String): Boolean = {
    val tblName = TableName.valueOf(tableName)
    val status = hBaseAdmin.tableExists(tblName)
    status
  }

  def isCFPresent(tableName: String, cfName: String): Boolean = {
    val tblName = TableName.valueOf(tableName)
    val tblDesc = hBaseAdmin.getTableDescriptor(tblName)
    tblDesc.getFamiliesKeys.map(new String(_)).contains(cfName)
  }

//  def createTable(tableName: String): Unit = {
//    try {
//      if (!isTablePresent(tableName)) {
//        val hTableDesc = new HTableDescriptor(tableName)
//        hTableDesc.addFamily(new HColumnDescriptor("default"))
//        hBaseAdmin.createTable(hTableDesc)
//        println(s"$tableName table is created with default column families.")
//      } else {
//        println(s"$tableName table is already present.")
//      }
//    }catch{
//      case ex: Exception => {
//        ex.printStackTrace()
//      }
//    }
//  }

  def createTable(tableName: String, cfList: Set[String]): Unit = {
    println("---------------------------")
    println("in create hbase table method")
    try {
      if (!isTablePresent(tableName)) {
        println("---------------------------")
        println("hbase table not present")
        val hTableDesc = new HTableDescriptor(tableName)
        for (cf <- cfList) {
          hTableDesc.addFamily(new HColumnDescriptor(cf))
        }
        hBaseAdmin.createTable(hTableDesc)
        println(s"$tableName table is created with ${cfList.mkString(",")} column families.")
      } else {
        println(s"$tableName table is already present with ${cfList.mkString(",")} column families.")
        cfList.foreach(cfName => {
          if (!isCFPresent(tableName, cfName)) {
            addCF(tableName, cfName)
          }
        })
      }
    }catch{
      case ex: Exception => {
      ex.printStackTrace()
      }
    }
  }

  def getTable(tableName: String): Table = {
    val tblName = TableName.valueOf(tableName)
    var hTable: Table = null
    try{
      hTable = hTableMap.getOrElseUpdate(tableName, connection.getTable(tblName))
      sys.addShutdownHook(
        hTable.close()
      )
    }catch{
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    hTable
  }

  def addCF(tableName: String, cfName: String): Unit = {
    val tblName = TableName.valueOf(tableName)
    try {
      hBaseAdmin.addColumn(tblName, new HColumnDescriptor(cfName))
      println(s"$cfName column family is added to the $tableName")
    }catch{
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  def disableTable(tableName: String): Unit = {
    val tblName = TableName.valueOf(tableName)
    try {
      hBaseAdmin.disableTable(tblName)
      println(s"$tableName table is disabled.")
    }catch{
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  def dropTable(tableName: String): Unit = {
    disableTable(tableName)
    val tblName = TableName.valueOf(tableName)
    try {
      hBaseAdmin.deleteTable(tblName)
      println(s"$tableName table is deleted.")
    }catch{
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  def put(tableName: String, row: String, cfName: String, colName: String, value: String): Unit = {
    val tblName = TableName.valueOf(tableName)
    var hTable: Table = null
    try{
      hTable = hTableMap.getOrElseUpdate(tableName, connection.getTable(tblName))
      val put = new Put(row.getBytes)
      put.addColumn(cfName.getBytes, colName.getBytes, value.getBytes())

      hTable.put(put)
      println("The data has been inserted into $tableName table")
    }catch{
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

//  def putBatch(tableName: String, putList: List[Put]): Unit = {
//    val tblName = TableName.valueOf(tableName)
//    var hTable: Table = null
//    try{
//      hTable = hTableMap.getOrElseUpdate(tableName, connection.getTable(tblName))
//      hTable.put(putList)
//      println(s"batch of put data has been inserted into $tableName table")
//    }catch{
//      case ex: Exception => {
//        ex.printStackTrace()
//      }
//    }
//  }

  def get(tableName: String, rowKey: String, cfName: String): Result = {
    val tblName = TableName.valueOf(tableName)
    var hTable: Table = null
    var result: Result = null
    try{
      hTable = hTableMap.getOrElseUpdate(tableName, connection.getTable(tblName))
      val get = new Get(rowKey.getBytes)
      get.addFamily(cfName.getBytes)
      get.getFamilyMap
      result = hTable.get(get)
    }catch{
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    result
  }

//  def scanTable(tableName: String): ResultScanner = {
//    val tblName = TableName.valueOf(tableName)
//    var hTable: Table = null
//    var results: ResultScanner = null
//    try {
//      hTable = hTableMap.getOrElseUpdate(tableName, connection.getTable(tblName))
//      val scan = new Scan()
//      scan.setMaxVersions(1)
//      results = hTable.getScanner(scan)
//      sys.addShutdownHook(
//        results.close()
//      )
//    }catch{
//      case ex: Exception => {
//        ex.printStackTrace()
//      }
//    }
//    results
//  }
//
//  def scanTable(tableName: String, cfName: String): ResultScanner = {
//    val tblName = TableName.valueOf(tableName)
//    var hTable: Table = null
//    var results: ResultScanner = null
//    try{
//      hTable = hTableMap.getOrElseUpdate(tableName, connection.getTable(tblName))
//      val scan = new Scan()
//      scan.setMaxVersions(1)
//      scan.addFamily(cfName.getBytes)
//      results = hTable.getScanner(scan)
//      sys.addShutdownHook(
//        results.close()
//      )
//    }catch{
//      case ex: Exception => {
//        ex.printStackTrace()
//      }
//    }
//    results
//  }
//
//  def scanTable(tableName: String, cfName: String, pred: String): ResultScanner = {
//    val tblName = TableName.valueOf(tableName)
//    var hTable: Table = null
//    var results: ResultScanner = null
//    try{
//      hTable = hTableMap.getOrElseUpdate(tableName, connection.getTable(tblName))
//      val scan = new Scan()
//      scan.setMaxVersions(1)
//      scan.addFamily(cfName.getBytes)
//      val regExComp = new RegexStringComparator("^.*"+pred+".*$")
//      val subStringComp = new SubstringComparator(s"$pred")
//      val rowFilter = new RowFilter(CompareOp.EQUAL, regExComp)
//      scan.setFilter(rowFilter)
//      results = hTable.getScanner(scan)
//      sys.addShutdownHook(
//        results.close()
//      )
//    }catch {
//      case ex: Exception => {
//        ex.printStackTrace()
//      }
//    }
//    results
//  }

}

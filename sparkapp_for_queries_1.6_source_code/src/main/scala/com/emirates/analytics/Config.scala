package com.emirates.analytics

import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
  * Created by Vijaya.Ramisetti on 28/01/2018.
  */

case class Config(airportsLoc: String,
                  carriersLoc: String,
                  planedateLoc:String,
                  otpLoc: String
                 )
object Config {
  def parse(args: Seq[String]): Config = {
    val scallop = new ScallopConf(args) {}
    val airportLoc: ScallopOption[String] = scallop.opt[String](name = "airports-path", required = true)
    val carriersLoc: ScallopOption[String] = scallop.opt[String](name = "carriers-path", required = true)
    val planedateLoc: ScallopOption[String] = scallop.opt[String](name = "planedate-path", required = true)
    val otpLoc: ScallopOption[String] = scallop.opt[String](name = "otp-path", required = true)

    scallop.verify()

    Config(
      airportLoc(),
      carriersLoc(),
      planedateLoc(),
      otpLoc()
    )
  }
}
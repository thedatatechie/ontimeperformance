package com.emirates.parquetconverter

import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
  * Created by Vijaya.Ramisetti on 28/01/2018.
  */

case class Config(inputLoc: String,
                  outputLoc: String
                 )

object Config {
  def parse(args: Seq[String]): Config = {
    val scallop = new ScallopConf(args) {}
    val inputLoc: ScallopOption[String] = scallop.opt[String](name = "input-path", required = true)
    val outputLoc: ScallopOption[String] = scallop.opt[String](name = "output-path", required = true)

    scallop.verify()

    Config(
      inputLoc(),
      outputLoc()
    )
  }
}
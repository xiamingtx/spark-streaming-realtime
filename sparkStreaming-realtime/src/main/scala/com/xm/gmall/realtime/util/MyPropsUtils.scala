package com.xm.gmall.realtime.util

import java.util.ResourceBundle

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object MyPropsUtils {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(propsKey: String): String = {
    bundle.getString(propsKey)
  }
}

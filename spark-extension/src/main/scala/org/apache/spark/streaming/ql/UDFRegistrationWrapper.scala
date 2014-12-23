/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.ql

import scala.reflect.runtime.universe.TypeTag

/**
 * UDF function registration wrapper
 */
private[ql] trait UDFRegistrationWrapper {
  self: StreamQLConnector =>

  // scalastyle:off
  def registerFunction[T: TypeTag](name: String, func: Function1[_, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function2[_, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function3[_, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function4[_, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function5[_, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function6[_, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function7[_, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function8[_, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function9[_, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function10[_, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function11[_, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function12[_, _, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function13[_, _, _, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }

  def registerFunction[T: TypeTag](name: String, func: Function22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): Unit = {
    self.registerFunction(name, func)
  }
  // scalastyle:on
}

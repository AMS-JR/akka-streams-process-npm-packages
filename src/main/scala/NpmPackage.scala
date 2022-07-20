import ujson._

import scala.collection.mutable.ArrayBuffer
case class NpmPackage(name: String){
  //version list
  var versions: List[Version] = List()
  // api request
  def fetchPackage: NpmPackage = {
    val response = requests.get(s"https://registry.npmjs.org/${name}")
//val response = requests.get(s"https://registry.npmjs.org/jasmine")
    if(response.statusCode == 200) {
      val data = ujson.read(response.text())
      for ((version, rest) <- data("versions").obj.toList) {
        versions = versions :+ Version(version, rest)
      }
    } else println(s"fetch request failed with Status code: ${response.statusCode}")
    this
  }
}
// base dependency
case class Dependency(packageName: String, version: String, dependencyType: String)
// dependency count with the keywords. I passed keywords here to make life easy
case class DependencyCount(packageName: String = "", version: String = "",
                           var dependency: Int = 0, var devDependency: Int = 0, keywords: ArrayBuffer[Value])
//Keywords -> Not in use. I should remove
case class Keywords(packageName: String, version: String, words: ArrayBuffer[Value])
// base version
//two parameters for the version number and body. NB Value in ujson can be of type Any
case class Version(version: String, objectBody: Value) {
  //dependency list
  var dependencies: List[Dependency] = List()
  val packageName: String = objectBody("name").str
  // keywords
  var keywords: ArrayBuffer[Value] = ArrayBuffer[Value]()
  //seperate dependency list appending since some packages have no devDependencies

  try {
    for ((version, rests) <- objectBody.obj("dependencies").obj.toList) {
      dependencies = dependencies :+ Dependency(packageName, version, "runtime")
    }
  } catch {
    case e: Exception =>
  }

  try {
    for ((version, rest) <- objectBody.obj("devDependencies").obj.toList) {
      dependencies = dependencies :+ Dependency(packageName, version, "dev")
    }
  } catch {
    case e: Exception =>
  }

  try {
     objectBody.obj("keywords").arrOpt match {
     case Some(u) => keywords = keywords  ++ u
     case None =>
 }
  } catch {
     case e: Exception =>
  }
}
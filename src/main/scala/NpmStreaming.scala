import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, FlowShape, Graph, IOResult, OverflowStrategy}
import akka.stream.scaladsl.{Balance, Broadcast, Compression, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}
import akka.util.ByteString
import ujson.Value

import java.nio.file.{Path, Paths}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

object NpmStreaming extends App {
  implicit val actorSystem: ActorSystem = ActorSystem("NpmStreaming")
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  val resourcesFolder: String = "src/main/resources"
  val pathTXTFile: Path = Paths.get(s"$resourcesFolder/packages.txt.gz")
  val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(pathTXTFile)

  //unzip the file
  val flowUnzip: Flow[ByteString, ByteString, NotUsed] = Compression.gunzip()
  //stringify
  val flowStringify: Flow[ByteString, String, NotUsed] = Flow[ByteString].map(_.utf8String)
  //split into a list
  val flowSplitWords: Flow[String, String, NotUsed] = Flow[String].mapConcat(_.split("\n").toList)
  //objectify
  val flowObjectify: Flow[String, NpmPackage, NotUsed] = Flow[String].map(NpmPackage)
  //pass objects to new source
  val newSource:  Flow[ByteString, NpmPackage, NotUsed] = flowUnzip
    .via(flowStringify)
    .via(flowSplitWords)
    .via(flowObjectify)
  //buffer streaming package
  val flowBuffer = Flow[NpmPackage].buffer(10, OverflowStrategy.backpressure)
  //limit package request ---this is where a single package is return!!!!!!!
  val flowRequestLimiter = Flow[NpmPackage].throttle(1, 3.second)
  //Api request and extract versions
  val flowFetchApi: Flow[NpmPackage, NpmPackage, NotUsed] = Flow[NpmPackage].map(_.fetchPackage)
  //Versions
  val flowMapVersions: Flow[NpmPackage, Version, NotUsed] = Flow[NpmPackage].mapConcat(_.versions)
  //Flow Dependencies
  val flowDependencies: Graph[FlowShape[Version, DependencyCount], NotUsed] = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      //Balance: emits upstream elements to the available outputs
      val dispatchVersions = builder.add(Balance[Version](2))
      //filter pipeline
      val flowFilterDependencies: Graph[FlowShape[Version, (DependencyCount, DependencyCount)], NotUsed] = Flow.fromGraph(
        GraphDSL.create() { implicit builder =>
          import GraphDSL.Implicits._
          //broadcast dependencies
          val broadcast = builder.add(Broadcast[Version](2))
          // objects of dependency counts for both runtime and dev dependencies
          val objectCounter = builder.add(Zip[DependencyCount, DependencyCount])

          //runtime dependencies
          val runtimeDependency: Flow[Version, DependencyCount, NotUsed] =
            Flow[Version].map({ version =>
              DependencyCount(version.packageName, version.version, dependency = version.dependencies.count(x => x.dependencyType == "runtime"),
                keywords = version.keywords)
            })
          //dev dependencies
          val devDependency: Flow[Version, DependencyCount, NotUsed] =
            Flow[Version].map({ version =>
              DependencyCount(version.packageName, version.version, devDependency = version.dependencies.count(x => x.dependencyType == "dev"),
                keywords = version.keywords)
            })

          broadcast.out(0) ~> runtimeDependency ~> objectCounter.in0
          broadcast.out(1) ~> devDependency ~> objectCounter.in1
          FlowShape(broadcast.in, objectCounter.out)
        })
      //combine dependency filters to a single DependencyCount instance
      val flowCombinedDependencyCount: Flow[(DependencyCount, DependencyCount), DependencyCount, NotUsed] =
        Flow[(DependencyCount, DependencyCount)].map(dc => {
          dc._1.devDependency = dc._2.devDependency
          dc._1
        })
      //merge dependencies
      val merge = builder.add(Merge[DependencyCount](2))

      dispatchVersions.out(0) ~> flowFilterDependencies ~> flowCombinedDependencyCount ~> merge.in(0)
      dispatchVersions.out(1) ~> flowFilterDependencies ~> flowCombinedDependencyCount ~> merge.in(1)
      FlowShape(dispatchVersions.in, merge.out)
    })
  //buffer versions
  val versionBuffer = Flow[Version].buffer(12, OverflowStrategy.backpressure)
//  //Sink
  var check: Boolean = true
  var pName: String = ""
  var keywords_base: ArrayBuffer[Value] = ArrayBuffer[Value]()
  val sink: Sink[DependencyCount, Future[Done]] = Sink.foreach(x => {

    if(check == true || !pName.equals(x.packageName) ) {
      check = false; pName = x.packageName
      println("Analysing " + x.packageName)
    }
    print("Version: " + x.version + ", Dependencies: " + x.dependency + ", DevDependencies: " + x.devDependency)
    if(keywords_base.isEmpty && !x.keywords.isEmpty) {
      keywords_base = keywords_base ++ x.keywords
      print(", Keywords: \"")
          for(i <- 0 to x.keywords.length-1){
            print(s"+${x.keywords(i).str}")
            if (i != x.keywords.length-1) print(", ")
          }
      print("\"")
    } else{
      for(i <- 0 to x.keywords.length-1) {
        if (!keywords_base.contains(x.keywords(i))) {
          keywords_base += (x.keywords(i))
          print(s", Keywords: +${x.keywords(i)}")
        }
      }
      for(j <- 0 to keywords_base.length-1){
          if(!x.keywords.contains(keywords_base(j))){
            keywords_base -= (keywords_base(j))
            print(s", Keywords: -${keywords_base(j)}")
          }
        }
      }
    println("\n")
  })
  val runnableGraph: RunnableGraph[Future[IOResult]] = source
    .via(newSource)
    .via(flowBuffer)
    .via(flowRequestLimiter)
    .via(flowFetchApi)
    .via(flowMapVersions)
    .via(versionBuffer)
    .via(flowDependencies)
    .to(sink)
  runnableGraph.run().foreach(_ => actorSystem.terminate())
}
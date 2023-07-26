import java.net.ServerSocket
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame

import scala.util.Try

object ServerApp {
  def loadModel(modelPath: String): PipelineModel = {
    PipelineModel.load(modelPath)
  }

  def processStream(rdd: DataFrame, model: PipelineModel, clientSocket: Socket): Unit = {
    import org.apache.spark.sql.functions._
    import clientSocket._

    import spark.implicits._

    // Convertir el DataFrame a un RDD de vectores (features)
    val featuresRDD = rdd.map(row => row.getAs[Vector](0))

    // Escalar las características para mejorar la convergencia (misma escala que se usó durante el entrenamiento)
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)

    val scaledFeaturesRDD = scaler.transform(featuresRDD.toDF()).select("scaledFeatures").as[Vector].rdd

    // Hacer las predicciones con el modelo
    val predictionsRDD = model.transform(scaledFeaturesRDD.toDF()).select("prediction").as[Double].rdd

    // Enviar las predicciones al cliente
    val predictionsArray = predictionsRDD.collect()
    val out = new ObjectOutputStream(clientSocket.getOutputStream)
    out.writeObject(predictionsArray)
    out.flush()
  }

  def handleClient(clientSocket: Socket, model: PipelineModel): Unit = {
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val lines = ssc.socketTextStream("192.168.0.108", 1960)

    lines.foreachRDD((rdd, time) => {
      if (!rdd.isEmpty) {
        processStream(rdd, model, clientSocket)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def startServer(model: PipelineModel): Unit = {
    val serverSocket = new ServerSocket(1960)
    println("Esperando conexiones de clientes...")

    while (true) {
      val clientSocket = serverSocket.accept()
      println(s"Cliente conectado: ${clientSocket.getInetAddress}")

      val clientThread = new Thread {
        override def run(): Unit = {
          Try(handleClient(clientSocket, model))
          clientSocket.close()
        }
      }

      clientThread.start()
    }
  }

  def main(args: Array[String]): Unit = {
    val modelPath = "path/to/your/model"
    val model = loadModel(modelPath)
    startServer(model)
  }
}

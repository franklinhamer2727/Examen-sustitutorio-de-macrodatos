import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.net._
import java.io._
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.feature.VectorAssembler

object StreamingServer {
  def main(args: Array[String]): Unit = {
    // Cargar el modelo MLP entrenado
    val mlp_model = MultilayerPerceptronClassificationModel.load("mlp_model")

    // Configurar el contexto de Spark Streaming
    val sparkConf = new SparkConf().setAppName("StreamingApp")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1)) // Intervalo de 1 segundo

    // Establecer la conexión con el cliente
    val serverSocket = new ServerSocket(1950)

    println("Esperando conexiones de clientes...")

    // Función para procesar el DStream y enviar la predicción al cliente
    def processStream(rdd: RDD[String]): Unit = {
      val data_array = rdd.collect()
      if (data_array.length > 0) {
        val prediction = mlp_model.predict(Vectors.dense(data_array(0).split(",").map(_.toDouble)))
        println(s"Predicción: $prediction")

        // Enviar la predicción al cliente
        val clientSocket = new Socket("192.168.0.108", 1950)
        val out = new PrintWriter(clientSocket.getOutputStream(), true)
        out.println(prediction)
        clientSocket.close()
      }
    }

    // Crear un DStream que recibe datos del cliente
    val lines = ssc.socketTextStream("192.168.0.108", 1950) // No es necesario especificar la dirección, ya que es el cliente actual

    // Procesar cada RDD del DStream
    lines.foreachRDD(processStream)

    // Iniciar el contexto de Spark Streaming
    ssc.start()

    try {
      // Mantener el contexto en ejecución
      ssc.awaitTermination()
    } catch {
      // Detener el contexto de Spark Streaming si se presiona Ctrl + C
      case _: InterruptedException =>
        println("Deteniendo el contexto de Spark Streaming...")
        ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }
}
